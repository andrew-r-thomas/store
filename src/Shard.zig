const Self = @This();

const std = @import("std");
const mem = std.mem;
const heap = std.heap;
const debug = std.debug;
const print = debug.print;

const format = @import("format.zig");
const Mesh = @import("Mesh.zig");

mem_table: MemTable,
block_server: BlockServer,
inner_offsets: std.AutoArrayHashMapUnmanaged(u64, u64),
leaf_offsets: std.AutoArrayHashMapUnmanaged(u64, u64),
root: u64,
mesh: *Mesh,

pump_arena: heap.ArenaAllocator,

pub fn init(
    cfg: Config,
    mesh: *Mesh,
    allocator: mem.Allocator,
) !Self {
    const pump_arena = heap.ArenaAllocator.init(allocator);
    const mem_table = try MemTable.init(
        cfg.max_page_size,
        cfg.block_size,
        allocator,
    );
    const block_server = try BlockServer.init(
        cfg.block_size,
        cfg.num_blocks,
        allocator,
    );
    const inner_offsets = std.AutoArrayHashMapUnmanaged(u64, u64).empty;
    const leaf_offsets = std.AutoArrayHashMapUnmanaged(u64, u64).empty;
    return Self{
        .mesh = mesh,
        .pump_arena = pump_arena,
        .mem_table = mem_table,
        .block_server = block_server,
        .inner_offsets = inner_offsets,
        .leaf_offsets = leaf_offsets,
        .root = 1,
    };
}
pub fn deinit(self: *Self) void {
    // mesh init/deinit is handled by central
    self.pump_arena.deinit();
    self.mem_table.deinit();
}

pub fn pump(self: *Self) void {
    _ = findLeaf(
        &.{ 0, 0, 0, 1 },
        &self.block_server,
        &self.inner_offsets,
        self.root,
    );
    _ = executeRead(
        &.{ 0, 0, 0, 1 },
        12,
        128,
        &self.block_server,
        &self.leaf_offsets,
    );
    _ = self.mem_table.read(true, 123);
    _ = self.mem_table.write(format.Commit, format.Commit{
        .timestamp = 123,
        .write = format.Write{ .key = &.{0}, .val = null },
    }) catch unreachable;
}

pub const Config = struct {
    block_size: usize,
    num_blocks: usize,
    max_page_size: usize,
};

pub const PageStore = struct {
    offset_table: std.HashMapUnmanaged(u64, u64),
    root: u64,
    next_pid: u64,

    pub fn init() @This() {
        return @This(){};
    }
    pub fn deinit(_: *@This(), _: mem.Allocator) void {}

    pub fn readBlock(
        _: *@This(),
        _: []const u8,
        _: u64,
        _: *PageCache,
    ) void {}
};

pub const PageCache = struct {
    buf_pool: []Buffer,
    free_list: std.ArrayListUnmanaged(usize),
    id_map: std.AutoHashMapUnmanaged(u64, usize),

    hits: [][2]u64,
    hit: u64,
    free_cap_target: usize,

    pub fn init(
        page_size: usize,
        pool_size: usize,
        free_cap_target: usize,
        allocator: mem.Allocator,
    ) !@This() {
        const buf = try allocator.alloc(u8, page_size * pool_size);
        var buf_pool = try allocator.alloc(Buffer, pool_size);
        var free_list = try std.ArrayListUnmanaged(usize).initCapacity(
            allocator,
            pool_size,
        );
        var id_map = std.AutoHashMapUnmanaged(u64, usize).empty;
        try id_map.ensureTotalCapacity(allocator, @intCast(pool_size));

        var hits = try allocator.alloc([2]u64, pool_size);

        for (0..pool_size) |i| {
            buf_pool[i] = Buffer.init(
                buf[i * page_size .. (i + 1) * page_size],
            );
            free_list.appendAssumeCapacity(i);
            hits[i] = .{ std.math.maxInt(u64), 0 };
        }

        return @This(){
            .buf_pool = buf_pool,
            .free_list = free_list,
            .id_map = id_map,

            .hits = hits,
            .hit = 1,
            .free_cap_target = free_cap_target,
        };
    }
    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        var og_buf = self.buf_pool[0].buf;
        og_buf.len = og_buf.len * self.buf_pool.len;
        allocator.free(og_buf);
        allocator.free(self.buf_pool);

        self.free_list.deinit(allocator);
        self.id_map.deinit(allocator);

        allocator.free(self.hits);
    }

    pub fn get(self: *@This(), pid: u64) ?usize {
        if (self.id_map.get(pid)) |idx| {
            self.hits[idx][1] = self.hits[idx][0];
            self.hits[idx][0] = self.hit;
            self.hit += 1;
            return idx;
        } else {
            return null;
        }
    }
    pub fn insert(self: *@This(), pid: u64, idx: usize) void {
        if (self.id_map.fetchPutAssumeCapacity(pid, idx)) {
            unreachable;
        }
        self.hits[idx][0] = self.hit;
        self.hits[idx][1] = 0;
        self.hit += 1;
    }

    pub fn remove(self: *@This(), pid: u64) void {
        if (!(self.id_map.fetchRemove(pid))) {
            unreachable;
        }
    }
    pub fn pop_free(self: *@This()) usize {
        if (self.free_list.pop()) |idx| {
            return idx;
        } else {
            unreachable;
        }
    }

    pub const Buffer = struct {
        buf: []u8,
        top: usize,

        const Error = error{
            BufFull,
        };

        pub fn init(buf: []u8) @This() {
            return @This(){ .buf = buf, .top = buf.len };
        }

        pub fn reset(self: *@This()) void {
            @memset(self.buf, 0);
            self.top = self.buf.len;
        }

        pub fn write(
            self: *@This(),
            comptime W: type,
            w: *const W,
        ) @This().Error!void {
            const size = w.size();
            if (size > self.top) {
                return @This().Error.BufFull;
            }
            w.serialize(self.buf[self.top - size .. self.top]);
            self.top -= size;
        }

        pub fn read(self: *const @This()) format.Page {
            return format.Page.fromBytes(self.buf[self.top..]);
        }
    };
};

/// ## TODO
/// - check memtable first
fn findLeaf(
    target: []const u8,
    block_server: *BlockServer,
    inner_offsets: *const std.AutoArrayHashMapUnmanaged(u64, u64),
    root: u64,
) union(findLeafTag) {
    needs_io: u64,
    leaf_id: u64,
} {
    var current = root;
    while (inner_offsets.get(current)) |o| {
        var offset: ?u64 = o;
        var best_op: ?format.Write = null;
        var best_entry: ?format.Entry = null;
        while (offset) |off| {
            const block_start = block_server.offsetToBlockStart(off);
            if (block_server.getBlock(block_start)) |block| {
                var chunk = chunkFromBlock(false, block, off);
                while (chunk.ops.next()) |write| {
                    switch (mem.order(u8, target, write.key)) {
                        .lt, .eq => {
                            if (best_op) |bo| {
                                if (mem.lessThan(u8, write.key, bo.key)) {
                                    best_op = write;
                                }
                            } else {
                                best_op = write;
                            }
                        },
                        .gt => {},
                    }
                }
                while (chunk.entries.next()) |entry| {
                    switch (mem.order(u8, target, entry.key)) {
                        .lt, .eq => {
                            best_entry = entry;
                            break;
                        },
                        .gt => {},
                    }
                }
                offset = chunk.next;
            } else {
                return .{ .needs_io = block_start };
            }
        }
        if (best_op) |bo| {
            if (best_entry) |be| {
                if (mem.lessThan(u8, be.key, bo.key)) {
                    current = format.PageId.fromBytes(be.val);
                } else {
                    current = format.PageId.fromBytes(bo.val.?);
                }
            } else {
                current = format.PageId.fromBytes(bo.val.?);
            }
        } else if (best_entry) |be| {
            current = format.PageId.fromBytes(be.val);
        } else unreachable;
    }

    return .{ .leaf_id = current };
}
const findLeafTag = enum {
    needs_io,
    leaf_id,
};

fn executeRead(
    target: []const u8,
    ts: u64,
    pid: u64,
    block_server: *BlockServer,
    leaf_offsets: *const std.AutoArrayHashMapUnmanaged(u64, u64),
) union(executeReadTag) {
    needs_io: u64,
    val: ?[]const u8,
} {
    var offset: ?u64 = leaf_offsets.get(pid) orelse unreachable;
    while (offset) |off| {
        const block_start = block_server.offsetToBlockStart(off);
        if (block_server.getBlock(block_start)) |block| {
            var chunk = chunkFromBlock(true, block, off);
            while (chunk.ops.next()) |commit| {
                if (commit.timestamp > ts) continue;
                if (mem.eql(u8, commit.write.key, target)) {
                    return .{ .val = commit.write.val };
                }
            }
            while (chunk.entries.next()) |entry| {
                if (mem.eql(u8, entry.key, target)) {
                    return .{ .val = entry.val };
                }
            }
            offset = chunk.next;
        } else {
            return .{ .needs_io = block_start };
        }
    }
    return .{ .val = null };
}
const executeReadTag = enum {
    needs_io,
    val,
};

fn chunkFromBlock(
    comptime is_leaf: bool,
    block: []const u8,
    offset: u64,
) format.PageChunk(is_leaf) {
    debug.assert((block.len & (block.len - 1)) == 0);
    const inner_offset: u64 = offset & (@as(u64, block.len) - 1);
    return format.PageChunk(is_leaf).fromBytes(block[inner_offset..]);
}

pub const MemTable = struct {
    page_map: std.AutoArrayHashMapUnmanaged(u64, Buffer),
    flush_arena: heap.ArenaAllocator,
    max_page_size: usize,
    total_size: usize,
    block_size: usize,

    pub const Buffer = struct {
        buf: []u8,
        top: usize,
        next: ?u64,
    };

    pub const Error = error{
        FULL,
    };

    pub fn init(
        max_page_size: usize,
        block_size: usize,
        allocator: mem.Allocator,
    ) !@This() {
        return @This(){
            .flush_arena = heap.ArenaAllocator.init(allocator),
            .page_map = std.AutoArrayHashMapUnmanaged(u64, Buffer).empty,
            .max_page_size = max_page_size,
            .total_size = 0,
            .block_size = block_size,
        };
    }
    pub fn deinit(self: *@This()) void {
        self.flush_arena.deinit();
    }

    pub fn read(
        self: *@This(),
        comptime is_leaf: bool,
        pid: u64,
    ) ?format.PageChunk(is_leaf) {
        const buffer = self.page_map.getPtr(pid) orelse return null;
        const header = format.PageChunkHeader{
            .len = buffer.buf.len - buffer.top,
            .next = buffer.next,
        };
        return format.PageChunk(is_leaf).fromParts(
            header,
            buffer.buf[buffer.top..],
        );
    }
    pub fn write(_: *@This(), comptime W: type, _: W) !void {}
    pub fn replace() void {}
};

pub const BlockServer = struct {
    blocks_buf: []u8,
    block_size: usize,

    mapping_table: std.AutoArrayHashMapUnmanaged(u64, usize),
    free_list: std.ArrayListUnmanaged(usize),

    const BlockPrio = struct {
        offset: u64,
        prio: u64,
    };

    pub fn init(
        block_size: usize,
        num_blocks: usize,
        allocator: mem.Allocator,
    ) !@This() {
        const blocks_buf = try allocator.alloc(u8, block_size * num_blocks);
        var mapping_table = std.AutoArrayHashMapUnmanaged(u64, usize).empty;
        try mapping_table.ensureTotalCapacity(allocator, num_blocks);
        var free_list = std.ArrayListUnmanaged(usize).empty;
        for (0..num_blocks) |i| {
            try free_list.append(allocator, i);
        }
        return @This(){
            .blocks_buf = blocks_buf,
            .block_size = block_size,
            .mapping_table = mapping_table,
            .free_list = free_list,
        };
    }
    pub fn deinit(_: *@This()) void {}

    pub inline fn offsetToBlockStart(self: *const @This(), offset: u64) u64 {
        return offset >> @intCast(@ctz(self.block_size));
    }
    pub fn getBlock(self: *@This(), block_start: u64) ?[]const u8 {
        const idx = self.mapping_table.get(block_start) orelse return null;
        const start = idx * self.block_size;
        const end = start + self.block_size;
        return self.blocks_buf[start..end];
    }

    pub fn popFree(self: *@This()) .{ usize, []u8 } {
        if (self.free_list.pop()) |i| {
            const start = i * self.block_size;
            const end = start + self.block_size;
            return .{ i, self.blocks_buf[start..end] };
        }
        unreachable;
    }
};

pub const Io = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {};

    const IoUring = std.os.linux.IoUring;
    const posix = std.posix;
    pub const Uring = struct {
        ring: IoUring,
        index_fd: posix.fd_t,
        block_io_vecs: []posix.iovec,
        ud_next: u64,

        pub fn init(
            entries: u16,
            blocks_buf: []u8,
            block_size: usize,
            index_file_path: []const u8,
            allocator: mem.Allocator,
        ) !@This() {
            var ring = try IoUring.init(entries);

            const num_blocks = blocks_buf.len / block_size;
            const block_io_vecs = try allocator.alloc(posix.iovec, num_blocks);
            for (0..num_blocks) |i| {
                block_io_vecs[i] = posix.iovec{
                    .base = blocks_buf.ptr + (i * block_size),
                    .len = block_size,
                };
            }
            try ring.register_buffers(block_io_vecs);

            const index_fd: posix.fd_t = try posix.open(
                index_file_path,
                posix.O.DIRECT | posix.O.RDWR | posix.O.CREAT,
                0o600, // private rw access
            );
            try ring.register_files(.{index_fd});

            return @This(){
                .ring = ring,
                .index_fd = index_fd,
                .block_io_vecs = block_io_vecs,
                .ud_next = 1,
            };
        }
        pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
            self.ring.deinit();
            allocator.free(self.block_io_vecs);
            self = undefined;
        }
        pub fn io(self: *@This()) Io {
            return Io{
                .ptr = self,
                .vtable = .{},
            };
        }

        pub fn readBlock(self: *@This(), offset: u64, buf_idx: u16) !void {
            try self.ring.read_fixed(
                self.ud_next,
                self.index_fd,
                &self.block_io_vecs[buf_idx],
                offset,
                buf_idx,
            );
        }
        pub fn writeBlock(self: *@This(), offset: u64, buf_idx: u16) !void {
            try self.ring.write_fixed(
                self.ud_next,
                self.index_fd,
                &self.block_io_vecs[buf_idx],
                offset,
                buf_idx,
            );
        }
    };
};
