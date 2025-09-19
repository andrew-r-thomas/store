const Self = @This();

const std = @import("std");
const mem = std.mem;
const heap = std.heap;
const debug = std.debug;
const print = debug.print;

const format = @import("format.zig");
const Zipper = @import("Zipper.zig");
const Executor = @import("Executor.zig");
const Mesh = @import("Mesh.zig");

root: Root,
levels: std.ArrayList(LevelMeta),
block_server: BlockServer,
conns: std.AutoArrayHashMap(u32, Conn),
zipper: Zipper,
mesh: *Mesh,
executor: Executor,

pump_arena: heap.ArenaAllocator,

/// ## TODO
/// - need the minimal structure when building new index
///   (root, then two inners, then two leaves each)
pub fn init(
    cfg: Config,
    allocator: mem.Allocator,
    mesh: *Mesh,
) !Self {
    const pump_arena = heap.ArenaAllocator.init(allocator);
    const root = try Root.init(allocator, cfg.block_size);
    const block_server = try BlockServer.init(
        cfg.block_size,
        cfg.num_blocks,
        allocator,
    );
    const zipper = Zipper.init(allocator, cfg.zip_cfg);
    return Self{
        .pump_arena = pump_arena,
        .root = root,
        .block_server = block_server,
        .mesh = mesh,
        .levels = std.ArrayListUnmanaged(LevelMeta).empty,
        .zipper = zipper,
        .executor = .init(allocator),
        .conns = .init(allocator),
    };
}
pub fn deinit(self: *Self) void {
    // mesh init/deinit is handled by central
    self.pump_arena.deinit();
    self.root.deinit();
}

pub fn pump(self: *Self) void {
    self.mesh.poll(
        1,
        struct {
            pub fn call(from: usize, first: []const Mesh.Msg, last: []const Mesh.Msg) usize {
                for (0..first.len + last.len) |i| {
                    const msg = if (i < first.len) first[i] else last[i - first.len];
                    switch (msg) {
                        .newConn => {},
                        .commitResp => {},
                        .writeReq => {},
                        else => unreachable,
                    }
                }

                return first.len + last.len;
            }
        },
    );
}

pub const Config = struct {
    block_size: usize,
    num_blocks: usize,
    max_page_size: usize,
    zip_cfg: Zipper.Cfg,
};

pub inline fn chunkFromBlock(
    comptime pt: format.page_type,
    block: []const u8,
    offset: u64,
) format.PageChunk(pt) {
    debug.assert((block.len & (block.len - 1)) == 0);
    const inner_offset: u64 = offset & (@as(u64, block.len) - 1);
    return format.PageChunk(pt).fromBytes(block[inner_offset..]);
}

/// this is the memtable and the root node of the tree
/// it only takes user ops/commits
pub const Root = struct {
    children: std.MultiArrayList(Child),

    gt_buf: Buffer,
    gt_pid: u64,

    flush_arena: heap.ArenaAllocator,
    total_size: usize,
    block_size: usize,

    pub const Child = struct {
        key: []const u8,
        lte_pid: u64,
        op_buf: Buffer,
    };

    pub const Error = error{
        FULL,
    };

    pub const Buffer = struct {
        buf: []u8,
        top: usize,

        pub const empty = @This(){
            .buf = &.{},
            .top = 0,
        };

        pub fn write(self: *@This(), comptime W: type, w: W) !void {
            const size = w.size();
            if (self.top < size) return Error.FULL;

            w.serialize(self.buf[self.top - size .. self.top]);
            self.top -= size;
        }
    };

    pub fn init(
        allocator: mem.Allocator,
        block_size: usize,
    ) !@This() {
        return @This(){
            .children = std.MultiArrayList(Child).empty,

            .gt_buf = Buffer.empty,
            .gt_pid = 0,

            .flush_arena = heap.ArenaAllocator.init(allocator),
            .total_size = 0,
            .block_size = block_size,
        };
    }
    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        self.children.deinit(allocator);
        self.flush_arena.deinit(allocator);
    }

    pub fn commit(self: *@This(), c: format.Commit) !void {
        const size = c.size();
        if ((size + self.total_size) > self.block_size) return Error.FULL;

        const alloc = self.flush_arena.allocator();
        const c_buf = try alloc.alloc(u8, size);
        c.serialize(c_buf);

        const child_idx = self.findChild(c.write.key);
        if (child_idx) |i| {
            try self.children.items(.op_buf)[i].write(format.Commit, c);
        } else {
            try self.gt_buf.write(format.Commit, c);
        }

        self.total_size += size;
    }
    pub fn read(
        self: *const @This(),
        target: []const u8,
        ts: u64,
    ) union(readRes) {
        pid: u64,
        val: ?[]const u8,
    } {
        const buf, const pid = if (self.findChild(target)) |i|
            .{
                &self.children.items(.op_buf)[i],
                self.children.items(.lte_pid)[i],
            }
        else
            .{ &self.gt_buf, self.gt_pid };

        var ops = format.Iter(format.Commit, false).fromBytes(
            buf.buf[buf.top..],
        );
        while (ops.next()) |c| {
            if (c.timestamp > ts) continue;
            if (mem.eql(u8, c.write.key, target)) {
                return .{ .val = c.write.val };
            }
        }

        return .{ .pid = pid };
    }
    const readRes = enum {
        pid,
        val,
    };

    pub fn flush(self: *@This(), block: []u8, level_meta: *LevelMeta) void {
        debug.assert(block.len == self.block_size);
        debug.assert(self.total_size <= self.block_size);

        var cursor: u64 = 0;

        for (
            self.children.items(.lte_pid),
            self.children.items(.op_buf),
        ) |pid, buf| {
            const off = level_meta.offset_table.getPtr(pid).?;

            // this will be an inner page most of the time,
            // but also it doesn't matter since it will be commits only
            const chunk = format.PageChunk(.inner){
                .chunk = .{
                    .commits = format.Iter(format.Commit, false).fromBytes(
                        buf.buf,
                    ),
                },
                .next = off.*,
            };

            off.* = level_meta.head + cursor;

            chunk.serialize(block[cursor .. cursor + chunk.size()]);
            cursor += chunk.size();
        }

        const off = level_meta.offset_table.getPtr(self.gt_pid).?;

        const chunk = format.PageChunk(.inner){
            .chunk = .{
                .commits = format.Iter(format.Commit, false).fromBytes(
                    self.gt_buf.buf,
                ),
            },
            .next = off.*,
        };

        off.* = level_meta.head + cursor;

        chunk.serialize(block[cursor .. cursor + chunk.size()]);

        debug.assert(self.flush_arena.reset(.retain_capacity));
    }

    pub fn insert(_: *@This(), _: format.Smop) void {}

    fn findChild(self: *const @This(), target: []const u8) ?usize {
        for (self.children.items(.key), 0..) |key, i| {
            switch (mem.order(u8, target, key)) {
                .lt, .eq => return i,
                .gt => {},
            }
        }
        return null;
    }
};

pub const LevelMeta = struct {
    level: usize,
    offset_table: std.AutoArrayHashMap(u64, u64),
    current_buf: BlockServer.Buffer,
    next_pid: u64,
    head: u64,
    tail: u64,
};

pub const BlockServer = struct {
    blocks_buf: []u8,
    block_size: usize,

    mapping_table: std.ArrayListUnmanaged(
        std.AutoArrayHashMapUnmanaged(u64, usize),
    ),
    free_list: std.ArrayListUnmanaged(usize),
    pins: []u16,

    const Buffer = struct {
        buf: []u8,
        off: usize,
        idx: usize,
    };

    const BlockPrio = struct {
        offset: u64,
        prio: u64,
    };
    pub const Error = error{
        needs_io,
    };

    pub fn init(
        block_size: usize,
        num_blocks: usize,
        allocator: mem.Allocator,
    ) !@This() {
        const blocks_buf = try allocator.alloc(u8, block_size * num_blocks);
        var mapping_table = std.ArrayListUnmanaged(
            std.AutoArrayHashMapUnmanaged(u64, usize),
        ).empty;
        try mapping_table.ensureTotalCapacity(allocator, num_blocks);
        var free_list = std.ArrayListUnmanaged(usize).empty;
        for (0..num_blocks) |i| {
            try free_list.append(allocator, i);
        }
        const pins = try allocator.alloc(u16, num_blocks);
        @memset(pins, 0);
        return @This(){
            .blocks_buf = blocks_buf,
            .block_size = block_size,
            .mapping_table = mapping_table,
            .free_list = free_list,
            .pins = pins,
        };
    }
    pub fn deinit(_: *@This()) void {}

    /// ## TODO
    /// - hit tracking
    pub fn getBlockIdx(
        self: *@This(),
        level: usize,
        offset: u64,
    ) !struct { usize, u64 } {
        const block_start = offset >> @intCast(@ctz(self.block_size));
        const idx = self.mapping_table.items[level].get(
            block_start,
        ) orelse return Error.needs_io;
        return .{ idx, block_start };
    }

    pub inline fn getBlock(self: *const @This(), idx: usize) []const u8 {
        const start = idx * self.block_size;
        const end = start + self.block_size;
        return self.blocks_buf[start..end];
    }

    pub fn flush() void {}
    pub inline fn pin(self: *@This(), idx: usize) void {
        self.pins[idx] += 1;
    }
    pub inline fn unpin(self: *@This(), idx: usize) void {
        self.pins[idx] -= 1;
    }

    /// returns a free mutable buffer and it's index.
    /// for now, panics if there are no free buffers.
    pub fn popFree(self: *@This()) .{ usize, []u8 } {
        if (self.free_list.pop()) |i| {
            const start = i * self.block_size;
            const end = start + self.block_size;
            return .{ i, self.blocks_buf[start..end] };
        }
        unreachable;
    }
};

pub const Conn = struct {};

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
