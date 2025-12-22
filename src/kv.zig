const std = @import("std");
const atomic = std.atomic;
const mem = std.mem;
const debug = std.debug;
const testing = std.testing;

const Io = std.Io;

const format = @import("format.zig");

pub const KvStore = struct {
    const Self = @This();

    io: std.Io,
    root: Root,

    pub fn init() Self {}
};

pub const Root = struct {
    children: atomic.Value(*[]Child),

    buffers: []CommitBuffer,
    free_list: std.ArrayList(usize),
    free_list_mu: std.Thread.Mutex,

    zip_lock: std.Thread.Mutex,
    zip_queue: *Io.Queue(Zipper.Message),
    io: Io,

    config: Config,

    const Self = @This();

    pub const Child = struct {
        pid: u64,
        /// the rightmost child has an empty slice for this value
        lte_key: []const u8,
        commit_chain_head: atomic.Value(usize),
    };

    pub const CommitBuffer = struct {
        buffer: SealBuffer(.prepend),
        next: atomic.Value(usize),
    };

    pub const Config = struct {
        /// if the number of buffers in the free list is <= this number, a zip will be triggered
        zip_threshold: usize,
    };

    pub fn init() Self {}
    pub fn deinit(_: *Self) void {}

    pub fn read(self: *const Self, key: []const u8, timestamp: u64) union(Tag) {
        pub const Tag = enum {
            val,
            pid,
        };
        val: ?[]const u8,
        pid: u64,
    } {
        const child = self.findChild(key);

        var buffer_idx = child.commit_chain_head.load(.acquire);
        while (buffer_idx < self.buffers.len) {
            const buffer = &self.buffers[buffer_idx];
            const data = buffer.buffer.read() catch break;
            defer buffer.buffer.doneReading();

            var iter = format.Iter(format.Commit).fromBytes(data);
            while (iter.next()) |c| {
                if (c.timestamp <= timestamp and c.write.key == key) {
                    return .{ .val = c.write.val };
                }
            }

            buffer_idx = buffer.next.load(.acquire);
        }

        return .{ .pid = child.pid };
    }

    /// the `timestamp` argument in calls to this function must be monotonically increasing,
    /// for example, once you pass `timestamp == 1`, you can no longer pass `timestamp == 0`
    pub fn commit(self: *const Self, write: format.Write, timestamp: u64) void {
        const child = self.findChild(write.key);
        const buffer_idx = child.commit_chain_head.load(.acquire);
        const buffer = &self.buffers[buffer_idx];

        const new_commit = format.Commit{ .ts = timestamp, .write = write };
        if (buffer.buffer.reserve(new_commit.size())) |res| switch (res) {
            .reservation => |reservation| {
                new_commit.serialize(reservation.buf);
                buffer.buffer.publish(reservation);
            },
            .seal => |seal| {
                self.free_list_mu.lock();
                const new_idx = self.free_list.pop().?;
                const len = self.free_list.items.len;
                self.free_list_mu.unlock();

                self.buffers[new_idx].next.store(buffer_idx, .release);
                child.commit_chain_head.store(new_idx, .release);

                buffer.buffer.waitForWriters(seal);

                if (len <= self.config.zip_threshold) {
                    if (self.zip_lock.tryLock()) {
                        self.zip_queue.putOneUncancelable();
                    }
                    // TODO trigger zip
                }

                self.commit(write, timestamp);
            },
        } else |_| self.commit(write, timestamp);
    }

    pub fn findChild(self: *const Self, key: []const u8) Child {
        const children = self.children.load(.acquire).*;
        var left = 0;
        var right = children.len - 2; // -2 because right most slot is > catch all
        while (left <= right) {
            const middle = left + ((right - left) / 2);
            const child = children[middle];
            if (child.lte_key < key) {
                left = middle + 1;
            } else {
                right = middle - 1;
            }
        }
        return children[left];
    }
};

pub fn SealBuffer(comptime mode: enum { append, prepend }) type {
    return struct {
        buf: []u8,
        reserved: atomic.Value(usize),
        published: atomic.Value(usize),
        readers: atomic.Value(usize),

        const Self = @This();

        pub const Reservation = struct {
            reserved: usize,
            buf: []u8,
        };
        pub const Seal = struct {
            reserved: usize,
        };

        pub const Error = error{
            SEALED,
        };

        pub fn init(capacity: usize, allocator: mem.Allocator) !Self {
            return Self{
                .buf = try allocator.alloc(u8, capacity),
                .reserved = .init(0),
                .published = .init(0),
                .readers = .init(0),
            };
        }
        pub fn deinit(self: *Self, allocator: mem.Allocator) void {
            allocator.free(self.buf);
            self.* = undefined;
        }

        pub fn reserve(self: *Self, size: usize) Error!union(Tag) {
            pub const Tag = enum {
                reservation,
                seal,
            };
            reservation: Reservation,
            seal: Seal,
        } {
            const reserved = self.reserved.fetchAdd(size, .acq_rel);
            if (reserved + size >= self.buf.len) {
                if (reserved >= self.buf.len) {
                    // already sealed
                    return Error.SEALED;
                }
                // we sealed
                return .{ .seal = .{ .reserved = reserved } };
            }

            const start = switch (mode) {
                .append => reserved,
                .prepend => (self.buf.len - reserved) - size,
            };
            return .{
                .reservation = .{ .reserved = reserved, .buf = self.buf[start .. start + size] },
            };
        }
        pub fn publish(self: *Self, reservation: Reservation) void {
            while (self.published.cmpxchgWeak(
                reservation.reserved,
                reservation.reserved + reservation.buf.len,
                .acq_rel,
                .monotonic,
            )) |_| {}
        }

        pub fn read(self: *Self) ![]const u8 {
            _ = self.readers.fetchAdd(1, .acq_rel);

            const published = self.published.load(.acquire);
            if (published >= self.buf.len) {
                // sealed to readers
                _ = self.readers.fetchSub(1, .acq_rel);
                return Error.SEALED;
            }

            const start = switch (mode) {
                .append => 0,
                .prepend => self.buf.len - published,
            };
            return self.buf[start .. start + published];
        }
        pub fn doneReading(self: *Self) void {
            _ = self.readers.fetchSub(1, .acq_rel);
        }

        pub fn waitForWriters(self: *const Self, seal: Seal) void {
            debug.assert(self.reserved.load(.acquire) >= self.buf.len);
            while (self.published.load(.acquire) != seal.reserved) {}
        }
        pub fn waitForReaders(self: *const Self) void {
            debug.assert(self.published.load(.acquire) > self.buf.len);
            while (self.readers.load(.acquire) > 0) {}
        }

        pub fn sealWriters(self: *Self) !Seal {
            const reserved = self.reserved.fetchAdd(self.buf.len, .acq_rel);
            if (reserved >= self.buf.len) {
                return Error.SEALED;
            }
            return .{ .reserved = reserved };
        }
        pub fn sealReaders(self: *Self, seal: Seal) void {
            debug.assert(self.published.load(.acquire) == seal.reserved);
            debug.assert(self.reserved.load(.acquire) >= self.buf.len);
            self.published.store(self.buf.len + 1, .release);
        }

        pub fn reset(self: *Self) void {
            debug.assert(self.published.load(.acquire) > self.buf.len);
            debug.assert(self.reserved.load(.acquire) >= self.buf.len);
            debug.assert(self.readers.load(.acquire) == 0);

            @memset(self.buf, 0);
            self.published.store(0, .release);
            self.reserved.store(0, .release);
        }
    };
}

test SealBuffer {
    {
        var buffer = try SealBuffer(.append).init(1024, testing.allocator);
        defer buffer.deinit(testing.allocator);

        switch (try buffer.reserve(64)) {
            .reservation => |reservation| {
                @memset(reservation.buf, 69);
                buffer.publish(reservation);
            },
            else => unreachable,
        }
        switch (try buffer.reserve(64)) {
            .reservation => |reservation| {
                @memset(reservation.buf, 42);
                buffer.publish(reservation);
            },
            else => unreachable,
        }

        const published = try buffer.read();
        defer buffer.doneReading();

        const expected = ([_]u8{69} ** 64) ++ ([_]u8{42} ** 64);
        try testing.expect(mem.eql(u8, published, &expected));
    }
    {
        var buffer = try SealBuffer(.prepend).init(1024, testing.allocator);
        defer buffer.deinit(testing.allocator);

        switch (try buffer.reserve(64)) {
            .reservation => |reservation| {
                @memset(reservation.buf, 69);
                buffer.publish(reservation);
            },
            else => unreachable,
        }

        switch (try buffer.reserve(64)) {
            .reservation => |reservation| {
                @memset(reservation.buf, 42);
                buffer.publish(reservation);
            },
            else => unreachable,
        }

        const published = try buffer.read();
        defer buffer.doneReading();

        const expected = ([_]u8{42} ** 64) ++ ([_]u8{69} ** 64);
        try testing.expect(mem.eql(u8, published, &expected));
    }
}

pub const Zipper = struct {
    pub const Message = union(Tag) {
        pub const Tag = enum {
            root,
        };

        root: *Root,
    };
};
