//! ## TODO
//! - eviction (when we figure out io)

const std = @import("std");
const mem = std.mem;
const testing = std.testing;

const format = @import("format.zig");

buf_pool: []Buffer,
free_list: std.ArrayListUnmanaged(usize),
id_map: std.AutoHashMapUnmanaged(u64, usize),

hits: [][2]u64,
hit: u64,
free_cap_target: usize,

const Self = @This();

pub fn init(
    page_size: usize,
    pool_size: usize,
    free_cap_target: usize,
    allocator: mem.Allocator,
) Self {
    const buf = allocator.alloc(u8, page_size * pool_size) catch unreachable;
    var buf_pool = allocator.alloc(Buffer, pool_size) catch unreachable;
    var free_list = std.ArrayListUnmanaged(usize).initCapacity(
        allocator,
        pool_size,
    ) catch unreachable;
    var id_map = std.AutoHashMapUnmanaged(u64, usize).empty;
    id_map.ensureTotalCapacity(
        allocator,
        @intCast(pool_size),
    ) catch unreachable;

    var hits = allocator.alloc([2]u64, pool_size) catch unreachable;

    for (0..pool_size) |i| {
        buf_pool[i] = Buffer.init(buf[i * page_size .. (i + 1) * page_size]);
        free_list.appendAssumeCapacity(i);
        hits[i] = .{ std.math.maxInt(u64), 0 };
    }

    return Self{
        .buf_pool = buf_pool,
        .free_list = free_list,
        .id_map = id_map,

        .hits = hits,
        .hit = 1,
        .free_cap_target = free_cap_target,
    };
}

pub fn deinit(self: *Self, allocator: mem.Allocator) void {
    var og_buf = self.buf_pool[0].buf;
    og_buf.len = og_buf.len * self.buf_pool.len;
    allocator.free(og_buf);
    allocator.free(self.buf_pool);

    self.free_list.deinit(allocator);
    self.id_map.deinit(allocator);

    allocator.free(self.hits);
}

pub fn get(self: *Self, pid: u64) ?usize {
    if (self.id_map.get(pid)) |idx| {
        self.hits[idx][1] = self.hits[idx][0];
        self.hits[idx][0] = self.hit;
        self.hit += 1;
        return idx;
    } else {
        return null;
    }
}

pub fn insert(self: *Self, pid: u64, idx: usize) void {
    if (self.id_map.fetchPutAssumeCapacity(pid, idx)) {
        unreachable;
    }
    self.hits[idx][0] = self.hit;
    self.hits[idx][1] = 0;
    self.hit += 1;
}

pub fn remove(self: *Self, pid: u64) void {
    if (!(self.id_map.fetchRemove(pid))) {
        unreachable;
    }
}

pub fn pop_free(self: *Self) usize {
    if (self.free_list.pop()) |idx| {
        return idx;
    } else {
        unreachable;
    }
}

pub const Buffer = struct {
    buf: []u8,
    top: usize,

    const _Self = @This();

    const Error = error{
        BufFull,
    };

    pub fn init(buf: []u8) _Self {
        return _Self{ .buf = buf, .top = buf.len };
    }

    pub fn reset(self: *_Self) void {
        @memset(self.buf, 0);
        self.top = self.buf.len;
    }

    pub fn write(
        self: *_Self,
        comptime W: type,
        w: *const W,
    ) _Self.Error!void {
        const size = w.size();
        if (size > self.top) {
            return _Self.Error.BufFull;
        }
        w.serialize(self.buf[self.top - size .. self.top]);
        self.top -= size;
    }

    pub fn read(self: *const _Self) format.Page {
        return format.Page.fromBytes(self.buf[self.top..]);
    }
};

test {
    var cache = Self.init(1024, 1024, 64, testing.allocator);
    defer cache.deinit(testing.allocator);
}
