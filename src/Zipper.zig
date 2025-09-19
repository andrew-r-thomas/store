//! ## NOTES
//! - we don't ever zip with the root as the parent, the root will update the
//!   top level pages priorities when it flushes, when we choose a top level
//!   page as a parent, we also compact the parent, and let the smops cascade
//!   up to the root, since we can modify that directly in memory easily
//! - our initial state will be an empty root pointing to two empty inner
//!   pages at the top level, each pointing respectively to two empty leaf
//!   pages, this way our structure is always in the normal operational shape
//!   we also don't really waste space doing bc of our LFS style file layout

const std = @import("std");

const mem = std.mem;
const heap = std.heap;

const debug = std.debug;
const print = debug.print;

const math = std.math;

const Shard = @import("Shard.zig");
const format = @import("format.zig");

const Self = @This();

state: State,
zip_prios: std.PriorityQueue(ZipRequest, ZipRequest.Ctx, ZipRequest.compare),
zip_arena: heap.ArenaAllocator,
pins: std.ArrayListUnmanaged(usize),
cfg: Cfg,

pub fn init(allocator: mem.Allocator, cfg: Cfg) Self {
    return Self{
        .state = .idle,
        .zip_prios = .init(allocator, .{}),
        .zip_arena = .init(allocator),
        .pins = .empty,
        .cfg = cfg,
    };
}

pub fn pump(
    self: *Self,
    block_server: *Shard.BlockServer,
    levels: *std.ArrayListUnmanaged(Shard.LevelMeta),
    root: *Shard.Root,
    oldest_active_ts: u64,
) !void {
    const alloc = self.zip_arena.allocator();

    // a wild stackless coroutine has appeared!
    state: switch (self.state) {
        .idle => {
            const req = self.zip_prios.removeOrNull() orelse return;
            self.state = .{
                .building_parent = .{
                    .req = req,
                    .next = levels.items[req.parent_level].offset_table.get(req.parent_id).?,
                    .parent = PageBuilder(.inner).init(req.parent_id),
                },
            };
            continue :state self.state;
        },
        .building_parent => |*data| {
            const block_idx, const block_start = block_server.getBlockIdx(
                data.req.parent_level,
                data.next,
            ) catch return;
            block_server.pin(block_idx);
            try self.pins.append(alloc, block_idx);

            const block = block_server.getBlock(block_idx);
            if (try data.parent.ingestChunk(
                alloc,
                Shard.chunkFromBlock(.inner, block, block_start),
            )) |next| {
                data.next = next;
                continue :state self.state;
            } else {
                const child_map = try data.parent.dumpCommits(alloc);
                self.state = .{
                    .zipping = .{ .req = data.req, .parent = data.parent, .child_map = child_map },
                };
                continue :state self.state;
            }
        },
        .zipping => |*data| {
            var parent_level = &levels.items[data.req.parent_level];
            if (data.child_map.pop()) |child| {
                const child_level = parent_level.level - 1;
                if (child_level == 0) {
                    self.state = .{
                        .building_leaf_child = .{
                            .zipping = data.*,
                            .new_child_commits = child.value,
                            .next = levels.items[child_level].offset_table.get(child.key).?,
                            .child = PageBuilder(.leaf).init(child.key),
                        },
                    };
                } else {
                    self.state = .{
                        .building_inner_child = .{
                            .zipping = data.*,
                            .new_child_commits = child.value,
                            .next = levels.items[child_level].offset_table.get(child.key).?,
                            .child = PageBuilder(.inner).init(child.key),
                        },
                    };
                }
                continue :state self.state;
            } else {
                if (data.req.parent_level == levels.items.len - 1) {
                    if (data.parent.size() > self.cfg.compact_thresh) {
                        try data.parent.compact(alloc, oldest_active_ts);
                        if (data.parent.size() > self.cfg.split_thresh) {
                            const to_pid = parent_level.next_pid;
                            parent_level.next_pid += 1;

                            var res = try data.parent.split(alloc, to_pid);
                            root.insert(res.smop);

                            const size = res.to.size();
                            const buf = parent_level
                                .current_buf
                                .buf[parent_level.current_buf.off..];
                            if (buf.len < size) {
                                // TODO: get a new buffer, and flush the current one
                                unreachable;
                            }
                            res.to.serialize(buf[0..size]);
                            try parent_level.offset_table.put(
                                res.to.pid,
                                parent_level.current_buf.off,
                            );
                            parent_level.current_buf.off += size;
                        }
                    }
                }

                const buf = parent_level.current_buf.buf[parent_level.current_buf.off..];
                const size = data.parent.size();
                if (buf.len < size) {
                    // TODO: get a new buffer, and flush the current one
                    unreachable;
                }
                data.parent.serialize(buf[0..data.parent.size()]);
                try parent_level.offset_table.put(
                    data.parent.pid,
                    parent_level.current_buf.off,
                );
                parent_level.current_buf.off += size;

                for (self.pins.items) |idx| {
                    block_server.unpin(idx);
                }
                debug.assert(self.zip_arena.reset(.retain_capacity));
                self.state = .idle;
                return;
            }
        },
        .building_inner_child => |*data| {
            var child_level = &levels.items[data.zipping.req.parent_level - 1];
            const block_idx, const block_start = block_server.getBlockIdx(
                child_level.level,
                data.next,
            ) catch return;
            block_server.pin(block_idx);
            try self.pins.append(alloc, block_idx);

            const block = block_server.getBlock(block_idx);
            if (try data.child.ingestChunk(
                alloc,
                Shard.chunkFromBlock(.inner, block, block_start),
            )) |next| {
                data.next = next;
                continue :state self.state;
            } else {
                if (data.child.size() > self.cfg.compact_thresh) {
                    try data.child.compact(alloc, oldest_active_ts);
                    if (data.child.size() > self.cfg.split_thresh) {
                        const to_pid = child_level.next_pid;
                        child_level.next_pid += 1;

                        var res = try data.child.split(alloc, to_pid);
                        try data.zipping.parent.smops.insert(alloc, 0, res.smop);

                        const size = res.to.size();
                        const buf = child_level.current_buf.buf[child_level.current_buf.off..];
                        if (buf.len < size) {
                            // TODO: get a new buffer, and flush the current one
                            unreachable;
                        }
                        res.to.serialize(buf[0..size]);
                        try child_level.offset_table.put(res.to.pid, child_level.current_buf.off);
                        child_level.current_buf.off += size;
                    }
                }

                const size = data.child.size();
                const buf = child_level.current_buf.buf[child_level.current_buf.off..];
                if (buf.len < size) {
                    // TODO: get a new buffer, and flush the current one
                    unreachable;
                }
                data.child.serialize(buf[0..size]);
                try child_level.offset_table.put(data.child.pid, child_level.current_buf.off);
                child_level.current_buf.off += size;

                self.state = .{ .zipping = data.zipping };
                continue :state self.state;
            }
        },
        .building_leaf_child => |*data| {
            var child_level = &levels.items[data.zipping.req.parent_level - 1];
            const block_idx, const block_start = block_server.getBlockIdx(
                child_level.level,
                data.next,
            ) catch return;
            block_server.pin(block_idx);
            try self.pins.append(alloc, block_idx);

            const block = block_server.getBlock(block_idx);
            if (try data.child.ingestChunk(
                alloc,
                Shard.chunkFromBlock(.leaf, block, block_start),
            )) |next| {
                data.next = next;
                continue :state self.state;
            } else {
                if (data.child.size() > self.cfg.compact_thresh) {
                    try data.child.compact(alloc, oldest_active_ts);
                    if (data.child.size() > self.cfg.split_thresh) {
                        const to_pid = child_level.next_pid;
                        child_level.next_pid += 1;

                        var res = try data.child.split(alloc, to_pid);
                        try data.zipping.parent.smops.insert(alloc, 0, res.smop);

                        const size = res.to.size();
                        const buf = child_level.current_buf.buf[child_level.current_buf.off..];
                        if (buf.len < size) {
                            // TODO: get a new buffer, and flush the current one
                            unreachable;
                        }
                        res.to.serialize(buf[0..size]);
                        try child_level.offset_table.put(res.to.pid, child_level.current_buf.off);
                        child_level.current_buf.off += size;
                    }
                }

                const size = data.child.size();
                const buf = child_level.current_buf.buf[child_level.current_buf.off..];
                if (buf.len < size) {
                    // TODO: get a new buffer, and flush the current one
                    unreachable;
                }
                data.child.serialize(buf[0..size]);
                try child_level.offset_table.put(data.child.pid, child_level.current_buf.off);
                child_level.current_buf.off += size;

                self.state = .{ .zipping = data.zipping };
                continue :state self.state;
            }
        },
    }
}

pub fn PageBuilder(comptime pt: format.page_type) type {
    return struct {
        pid: u64,
        commits: std.ArrayListUnmanaged(format.Commit),
        smops: std.ArrayListUnmanaged(format.Smop),
        entries: union(Tag) {
            none,
            chunk: ChunkEntries,
            new: NewEntries,

            pub const Tag = enum {
                none,
                chunk,
                new,
            };
        },

        pub const ChunkEntries = switch (pt) {
            .leaf => format.LeafEntries,
            .inner => format.InnerEntries,
        };
        pub const NewEntries = switch (pt) {
            .inner => struct {
                list: std.MultiArrayList(struct { pid: u64, key: []const u8 }),
                gt_pid: u64,

                pub const empty = @This(){
                    .list = .empty,
                    .gt_pid = 0,
                };
            },
            .leaf => std.MultiArrayList(struct { key: []const u8, val: []const u8 }),
        };

        pub fn init(pid: u64) @This() {
            return @This(){
                .pid = pid,
                .commits = .empty,
                .smops = .empty,
                .entries = .none,
            };
        }

        pub fn size(_: *const @This()) usize {
            return 0;
        }

        pub fn ingestChunk(
            self: *@This(),
            allocator: mem.Allocator,
            chunk: format.PageChunk(pt),
        ) !?u64 {
            switch (chunk.chunk) {
                .commits => |commits| {
                    var c = commits;
                    while (c.next()) |commit| {
                        try self.commits.append(allocator, commit);
                    }
                    return chunk.next;
                },
                .smops => |smops| {
                    var s = smops;
                    while (s.next()) |smop| {
                        try self.smops.append(allocator, smop);
                    }
                    return chunk.next;
                },
                .entries => |e| {
                    self.entries = .{ .chunk = e };
                    return null;
                },
            }
        }

        pub fn dumpCommits(
            self: *@This(),
            allocator: mem.Allocator,
        ) !std.AutoArrayHashMapUnmanaged(u64, std.ArrayListUnmanaged(format.Commit)) {
            comptime {
                debug.assert(pt == .inner);
            }

            var child_map = std.AutoArrayHashMapUnmanaged(
                u64,
                std.ArrayListUnmanaged(format.Commit),
            ).empty;
            commits: for (self.commits.items) |commit| {
                for (self.smops.items) |smop| {
                    const gt_order = mem.order(u8, commit.write.key, smop.gt_key);
                    const lte_order = mem.order(u8, commit.write.key, smop.lte_key);
                    if ((gt_order == .gt) and ((lte_order == .lt) or (lte_order == .eq))) {
                        const res = try child_map.getOrPut(allocator, smop.pid);
                        if (!res.found_existing) {
                            res.value_ptr.* = .empty;
                        }
                        try res.value_ptr.append(allocator, commit);
                        continue :commits;
                    }
                }
                const res = try child_map.getOrPut(
                    allocator,
                    self.entries.chunk.search(commit.write.key),
                );
                if (!res.found_existing) {
                    res.value_ptr.* = .empty;
                }
                try res.value_ptr.append(allocator, commit);
            }

            return child_map;
        }

        pub fn compact(self: *@This(), allocator: mem.Allocator, oldest_active_ts: u64) !void {
            switch (pt) {
                .inner => {
                    var new_entries = NewEntries.empty;
                    const old_entries = self.entries.chunk;

                    for (0..old_entries.key_offs.len) |i| {
                        const pid = old_entries.pids[i];
                        const key_off = old_entries.key_offs[i];
                        const key_len = old_entries.key_lens[i];
                        try new_entries.list.append(
                            allocator,
                            .{ .pid = pid, .key = old_entries.keys[key_off .. key_off + key_len] },
                        );
                    }
                    new_entries.gt_pid = old_entries.pids[old_entries.pids.len - 1];

                    // NOTE: since we're only doing splits right now, all smops are inserts
                    // TODO: adjust for merges
                    smops: for (self.smops.items) |smop| {
                        for (new_entries.list.items(.key), 0..) |key, i| {
                            if (mem.lessThan(u8, smop.lte_key, key)) {
                                try new_entries.list.insert(
                                    allocator,
                                    i,
                                    .{ .pid = smop.pid, .key = smop.lte_key },
                                );
                                continue :smops;
                            }
                        }
                        try new_entries.list.append(
                            allocator,
                            .{ .pid = smop.pid, .key = smop.lte_key },
                        );
                    }
                    self.smops = .empty;

                    var safe_point: ?usize = null;
                    for (self.commits.items, 0..) |commit, i| {
                        if (commit.timestamp < oldest_active_ts) {
                            safe_point = i;
                            break;
                        }
                    }
                    if (safe_point) |sp| {
                        var hit_keys = KeySet.empty;
                        var to_remove = std.ArrayListUnmanaged(usize).empty;
                        for (self.commits.items[sp..], 0..) |commit, i| {
                            if (try hit_keys.insert(allocator, commit.write.key)) {
                                try to_remove.append(allocator, i);
                            }
                        }
                        for (to_remove.items) |i| {
                            _ = self.commits.orderedRemove(i);
                        }
                    }

                    self.entries = .{ .new = new_entries };
                },
                .leaf => {
                    // TODO: this will change when we need to do sib ptr changes for range scans
                    debug.assert(self.smops.items.len == 0);
                    var new_entries = NewEntries.empty;
                    const old_entries = self.entries.chunk;

                    for (0..old_entries.offs.len) |i| {
                        const off = old_entries.offs[i];
                        const key_len = old_entries.key_lens[i];
                        const val_len = old_entries.val_lens[i];
                        try new_entries.append(
                            allocator,
                            .{
                                .key = old_entries.entries[off .. off + key_len],
                                .val = old_entries.entries[off + key_len .. off + key_len + val_len],
                            },
                        );
                    }

                    var safe_point: ?usize = null;
                    for (self.commits.items, 0..) |commit, i| {
                        if (commit.timestamp < oldest_active_ts) {
                            safe_point = i;
                            break;
                        }
                    }
                    if (safe_point) |sp| {
                        commits: for (self.commits.items[sp..]) |commit| {
                            for (new_entries.items(.key), 0..) |key, i| {
                                switch (mem.order(u8, commit.write.key, key)) {
                                    .lt => {
                                        if (commit.write.val) |val| {
                                            try new_entries.insert(
                                                allocator,
                                                i,
                                                .{ .key = commit.write.key, .val = val },
                                            );
                                        }
                                        continue :commits;
                                    },
                                    .eq => {
                                        if (commit.write.val) |val| {
                                            new_entries.set(
                                                i,
                                                .{ .key = commit.write.key, .val = val },
                                            );
                                        } else {
                                            new_entries.orderedRemove(i);
                                        }
                                        continue :commits;
                                    },
                                    .gt => {},
                                }
                            }
                            if (commit.write.val) |val| {
                                try new_entries.append(
                                    allocator,
                                    .{ .key = commit.write.key, .val = val },
                                );
                            }
                        }
                        self.commits.items = self.commits.items[0..sp];
                    }

                    self.entries = .{ .new = new_entries };
                },
            }
        }

        pub fn split(
            self: *@This(),
            allocator: mem.Allocator,
            to_pid: u64,
        ) !struct {
            smop: format.Smop,
            to: PageBuilder(pt),
        } {
            debug.assert(switch (self.entries) {
                .new => true,
                else => false,
            });
            debug.assert(self.smops.items.len == 0);

            var to = @This().init(to_pid);
            switch (pt) {
                .inner => {
                    const slice = self.entries.new.list.slice();
                    const middle = slice.get(slice.len / 2);
                    to.entries.new.gt_pid = middle.pid;
                    to.entries.new.list = slice.subslice(0, slice.len / 2).toMultiArrayList();
                    self.entries.new.list = slice.subslice(
                        (slice.len / 2) + 1,
                        slice.len,
                    ).toMultiArrayList();

                    var remove = std.ArrayListUnmanaged(usize).empty;
                    for (self.commits.items, 0..) |commit, i| {
                        switch (mem.order(u8, commit.write.key, middle.key)) {
                            .lt, .eq => {
                                try to.commits.append(allocator, commit);
                                try remove.append(allocator, i);
                            },
                            .gt => {},
                        }
                    }
                    self.commits.orderedRemoveMany(remove.items);

                    return .{
                        .smop = format.Smop{
                            .pid = to_pid,
                            .gt_key = to.entries.new.list.get(0).key,
                            .lte_key = middle.key,
                        },
                        .to = to,
                    };
                },
                .leaf => {
                    const slice = self.entries.new.slice();
                    const middle = slice.get(slice.len / 2);
                    to.entries.new = slice.subslice(0, (slice.len / 2) + 1).toMultiArrayList();
                    self.entries.new = slice.subslice(
                        (slice.len / 2) + 1,
                        slice.len,
                    ).toMultiArrayList();

                    var remove = std.ArrayListUnmanaged(usize).empty;
                    for (self.commits.items, 0..) |commit, i| {
                        switch (mem.order(u8, commit.write.key, middle.key)) {
                            .lt, .eq => {
                                try to.commits.append(allocator, commit);
                                try remove.append(allocator, i);
                            },
                            .gt => {},
                        }
                    }
                    self.commits.orderedRemoveMany(remove.items);

                    return .{
                        .smop = format.Smop{
                            .pid = to_pid,
                            .gt_key = to.entries.new.get(0).key,
                            .lte_key = middle.key,
                        },
                        .to = to,
                    };
                },
            }
        }

        pub fn serialize(_: *@This(), _: []u8) void {}
    };
}

pub const ZipRequest = struct {
    parent_id: u64,
    parent_level: usize,

    commit_depth: u64,

    pub const Ctx = struct {};

    pub fn compare(_: Ctx, a: @This(), b: @This()) math.Order {
        return math.order(a.commit_depth, b.commit_depth);
    }
};

pub const State = union(Tag) {
    idle,
    building_parent: BuildingParent,
    zipping: Zipping,
    building_inner_child: BuildingChild(.inner),
    building_leaf_child: BuildingChild(.leaf),

    pub const Tag = enum {
        idle,
        building_parent,
        zipping,
        building_inner_child,
        building_leaf_child,
    };

    pub const BuildingParent = struct {
        req: ZipRequest,
        next: u64,
        parent: PageBuilder(.inner),
    };
    pub const Zipping = struct {
        req: ZipRequest,
        parent: PageBuilder(.inner),
        child_map: std.AutoArrayHashMapUnmanaged(u64, std.ArrayListUnmanaged(format.Commit)),
    };
    pub fn BuildingChild(comptime pt: format.page_type) type {
        return struct {
            zipping: Zipping,
            new_child_commits: std.ArrayListUnmanaged(format.Commit),
            next: u64,
            child: PageBuilder(pt),
        };
    }
};

pub const Cfg = struct {
    /// the maximum number of total bytes a page's commit section can contain
    /// before being requested for a zip
    zip_thresh: u64,
    /// the maximum number of total bytes a page can contain before being
    /// compacted
    compact_thresh: u64,
    /// the maximum number of total bytes a page's entries section can contain
    /// before being split
    split_thresh: u64,
    /// the minimum number of total bytes a page's entries section can contain
    /// before being merged
    merge_thresh: u64,
};

pub const KeySet = struct {
    map: std.ArrayHashMapUnmanaged(
        []const u8,
        struct {},
        struct {
            pub fn hash(_: *const @This(), key: []const u8) u32 {
                var hasher = std.hash.Wyhash.init(0);
                std.hash.autoHashStrat(&hasher, key, .Deep);
                return @truncate(hasher.final());
            }
            pub fn eql(_: *const @This(), a: []const u8, b: []const u8, _: usize) bool {
                return mem.eql(u8, a, b);
            }
        },
        false,
    ),

    pub const empty = @This(){
        .map = .empty,
    };

    pub fn insert(self: *@This(), allocator: mem.Allocator, key: []const u8) !bool {
        const res = try self.map.getOrPut(allocator, key);
        res.value_ptr.* = .{};
        return res.found_existing;
    }
};
