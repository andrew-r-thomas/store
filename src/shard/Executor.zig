const Self = @This();

const std = @import("std");
const mem = std.mem;
const heap = std.heap;
const debug = std.debug;

const format = @import("store_lib").format;
const Shard = @import("Shard.zig");

txns: std.AutoArrayHashMap(Txn.Id, Txn),
pump_arena: heap.ArenaAllocator,

pub fn init(allocator: mem.Allocator) Self {
    return Self{ .txns = .init(allocator), .pump_arena = .init(allocator) };
}

pub fn pump(
    self: *Self,
    block_server: *Shard.BlockServer,
    root: *Shard.Root,
    levels: []Shard.LevelMeta,
) !void {
    const alloc = self.pump_arena.allocator();
    var reads = std.ArrayListUnmanaged(Read).empty;

    var txn_iter = self.txns.iterator();
    while (txn_iter.next()) |entry| {
        const txn = entry.value_ptr;
        if (txn.peekFrom()) |op| {
            switch (op) {
                .read => |read| try reads.append(alloc, .{ .read = read, .txn = txn }),
                .write => |write| {
                    try txn.pushWrite(write);
                    try txn.pushTo(.write);
                    txn.drainFrom(write.size());
                },
                .txn_ctrl => |_| {},
            }
        }
    }

    var current_level_reads = std.AutoArrayHashMapUnmanaged(
        u64,
        std.ArrayListUnmanaged(Read),
    ).empty;
    for (reads.items) |read| {
        switch (root.read(read.read.key, read.txn.start_ts)) {
            .pid => |pid| {
                var entry = try current_level_reads.getOrPut(alloc, pid);
                if (!entry.found_existing) {
                    entry.value_ptr.* = .empty;
                }
                try entry.value_ptr.append(alloc, read);
            },
            .val => |val| {
                try read.txn.pushTo(format.ResponseOp{ .read = val });
                read.txn.drainFrom(read.read.size());
            },
        }
    }

    var next_level_reads = std.AutoArrayHashMapUnmanaged(u64, std.ArrayListUnmanaged(Read)).empty;
    var current_level = levels.len - 1;
    while (current_level > 0) {
        while (current_level_reads.pop()) |entry| {
            const pid = entry.key;
            var page_reads = entry.value;
            var page_iter = PageIter(.inner){
                .next_off = levels[current_level].offset_table.get(pid),
                .level = current_level,
                .block_server = block_server,
            };

            while (page_iter.next()) |chunk| {
                var to_remove = std.ArrayListUnmanaged(usize).empty;
                switch (chunk.chunk) {
                    .commits => |commits| {
                        for (page_reads.items, 0..) |read, i| {
                            var c = commits;
                            while (c.next()) |commit| {
                                if (commit.timestamp <= read.txn.start_ts) {
                                    if (mem.eql(u8, commit.write.key, read.read.key)) {
                                        try read.txn.pushTo(.{ .read = commit.write.val });
                                        read.txn.drainFrom(read.read.size());
                                        try to_remove.append(alloc, i);
                                        break;
                                    }
                                }
                            }
                            c.reset();
                        }
                    },
                    .smops => |smops| {
                        for (page_reads.items, 0..) |read, i| {
                            var s = smops;
                            while (s.next()) |smop| {
                                const gt_order = mem.order(u8, read.read.key, smop.gt_key);
                                const lte_order = mem.order(u8, read.read.key, smop.lte_key);
                                if (gt_order == .gt and
                                    ((lte_order == .lt) or (lte_order == .eq)))
                                {
                                    const e = try next_level_reads.getOrPut(alloc, smop.pid);
                                    if (!e.found_existing) {
                                        e.value_ptr.* = .empty;
                                    }
                                    try e.value_ptr.append(alloc, read);
                                    try to_remove.append(alloc, i);
                                    break;
                                }
                            }
                            s.reset();
                        }
                    },
                    .entries => |entries| {
                        for (page_reads.items) |read| {
                            const next_pid = entries.search(read.read.key);
                            const e = try next_level_reads.getOrPut(alloc, next_pid);
                            if (!e.found_existing) {
                                e.value_ptr.* = .empty;
                            }
                            try e.value_ptr.append(alloc, read);
                        }
                    },
                }
                page_reads.orderedRemoveMany(to_remove.items);
            }
        }

        current_level_reads = next_level_reads;
        next_level_reads = .empty;
        current_level -= 1;
    }

    while (current_level_reads.pop()) |entry| {
        const pid = entry.key;
        var page_reads = entry.value;

        var page_iter = PageIter(.leaf){
            .next_off = levels[current_level].offset_table.get(pid),
            .level = current_level,
            .block_server = block_server,
        };

        while (page_iter.next()) |chunk| {
            var to_remove = std.ArrayListUnmanaged(usize).empty;
            switch (chunk.chunk) {
                .commits => |commits| {
                    for (page_reads.items, 0..) |read, i| {
                        var c = commits;
                        while (c.next()) |commit| {
                            if (commit.timestamp <= read.txn.start_ts) {
                                if (mem.eql(u8, commit.write.key, read.read.key)) {
                                    try read.txn.pushTo(.{ .read = commit.write.val });
                                    read.txn.drainFrom(read.read.size());
                                    try to_remove.append(alloc, i);
                                    break;
                                }
                            }
                        }
                        c.reset();
                    }
                },
                .entries => |entries| {
                    for (page_reads.items) |read| {
                        const val = entries.search(read.read.key);
                        try read.txn.pushTo(.{ .read = val });
                        read.txn.drainFrom(read.read.size());
                    }
                },
                .smops => unreachable,
            }
            page_reads.orderedRemoveMany(to_remove.items);
        }
    }

    debug.assert(self.pump_arena.reset(.retain_capacity));
}

pub fn PageIter(comptime pt: format.page_type) type {
    return struct {
        next_off: ?u64,
        level: usize,
        block_server: *Shard.BlockServer,

        pub fn next(self: *@This()) ?format.PageChunk(pt) {
            if (self.next_off) |next_off| {
                const block_idx, const block_start = self.block_server.getBlockIdx(
                    self.level,
                    next_off,
                ) catch return null;
                const block = self.block_server.getBlock(block_idx);
                const chunk = Shard.chunkFromBlock(pt, block, block_start);
                self.next_off = chunk.next;
                return chunk;
            } else {
                return null;
            }
        }
    };
}

pub const Read = struct {
    read: format.Read,
    txn: *Txn,
};
pub const Txn = struct {
    to: std.ArrayList(u8),
    from: std.ArrayList(u8),
    writes: std.ArrayList(u8),
    allocator: mem.Allocator,

    start_ts: u64,

    pub const Id = struct {
        conn_fd: u32,
        txn_id: u64,
    };

    pub fn peekFrom(self: *const @This()) ?format.RequestOp {
        if (self.from.items.len == 0) return null;
        return format.RequestOp.fromBytes(self.from.items);
    }

    pub fn drainFrom(self: *@This(), len: usize) void {
        debug.assert(len <= self.from.items.len);
        const old_len = self.from.items.len;
        @memmove(self.from.items[0 .. old_len - len], self.from.items[len..old_len]);
        self.from.items.len -= len;
    }

    pub fn pushTo(self: *@This(), resp: format.ResponseOp) !void {
        const old_len = self.to.items.len;
        try self.to.resize(self.allocator, old_len + resp.size());
        resp.serialize(self.to.items[old_len..]);
    }

    pub fn pushWrite(self: *@This(), write: format.Write) !void {
        const old_len = self.writes.items.len;
        try self.writes.resize(self.allocator, old_len + write.size());
        write.serialize(self.writes.items[old_len..]);
    }
};
