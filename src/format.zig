//! ## TODO
//! - convert all ints to little endian
//! - alignment tricks

const std = @import("std");

const mem = std.mem;
const heap = std.heap;

const debug = std.debug;
const assert = debug.assert;
const print = debug.print;

const testing = std.testing;

comptime {
    debug.assert(@import("builtin").target.cpu.arch.endian() == .little);
}

pub const FLAGS_SIZE = @sizeOf(u8);

pub const TYPE_MASK: u8 = 0b11_000000;

pub const Key = struct {
    pub const LEN_SIZE = @sizeOf(u16);

    const Self = []const u8;

    pub inline fn size(self: Self) usize {
        return LEN_SIZE + self.len;
    }

    pub inline fn serialize(self: Self, buf: []u8) void {
        assert(size(self) == buf.len);

        var cursor: usize = 0;

        mem.writeInt(
            u16,
            buf[cursor .. cursor + LEN_SIZE][0..LEN_SIZE],
            @intCast(self.len),
            .little,
        );
        cursor += LEN_SIZE;

        @memcpy(buf[cursor .. cursor + self.len], self);
    }

    pub inline fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < LEN_SIZE) {
            return Error.Set.EOF;
        }

        var cursor: usize = 0;
        const key_len = mem.readInt(
            u16,
            buf[cursor .. cursor + LEN_SIZE][0..LEN_SIZE],
            .little,
        );
        cursor += LEN_SIZE;

        if (buf.len < cursor + key_len) {
            return Error.Set.EOF;
        }

        return buf[cursor .. cursor + key_len];
    }
};

pub const Val = struct {
    pub const LEN_SIZE = @sizeOf(u32);

    const Self = ?[]const u8;

    pub const FLAG_SOME: u8 = 0b1_0000000;
    pub const FLAG_NONE: u8 = 0b0_0000000;

    pub const OPTION_MASK: u8 = 0b1_0000000;

    pub fn size(self: Self) usize {
        return FLAGS_SIZE + if (self) |v|
            LEN_SIZE + v.len
        else
            0;
    }

    pub fn flags(self: Self) u8 {
        return if (self) |_| FLAG_SOME else FLAG_NONE;
    }

    pub fn serialize(self: Self, buf: []u8) void {
        assert(size(self) == buf.len);

        var cursor: usize = 0;

        buf[cursor] = flags(self);
        cursor += FLAGS_SIZE;

        if (self) |v| {
            mem.writeInt(
                u32,
                buf[cursor .. cursor + LEN_SIZE][0..LEN_SIZE],
                @intCast(v.len),
                .little,
            );
            cursor += LEN_SIZE;

            @memcpy(buf[cursor .. cursor + v.len], v);
        }
    }

    pub fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < FLAGS_SIZE) {
            return Error.Set.EOF;
        }

        var cursor: usize = 0;

        const flgs = buf[cursor];
        switch (flgs & OPTION_MASK) {
            FLAG_SOME => {
                cursor += FLAGS_SIZE;

                if (buf.len < cursor + LEN_SIZE) {
                    return Error.Set.EOF;
                }

                const val_len = mem.readInt(
                    u32,
                    buf[cursor .. cursor + LEN_SIZE][0..LEN_SIZE],
                    .little,
                );
                cursor += LEN_SIZE;

                if (buf.len < cursor + val_len) {
                    return Error.Set.EOF;
                }

                return buf[cursor .. cursor + val_len];
            },
            FLAG_NONE => {
                return null;
            },
            else => unreachable,
        }
    }
};

pub const TxnId = struct {
    pub const SIZE = @sizeOf(u64);

    const Self = u64;

    pub inline fn serialize(self: Self, buf: []u8) void {
        assert(SIZE == buf.len);
        mem.writeInt(Self, buf[0..SIZE], self, .little);
    }

    pub inline fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < SIZE) {
            return Error.Set.EOF;
        }
        return mem.readInt(Self, buf[0..SIZE], .little);
    }
};

pub const PageId = struct {
    pub const SIZE = @sizeOf(u64);

    const Self = u64;

    pub inline fn serialize(self: Self, buf: []u8) void {
        assert(SIZE == buf.len);
        mem.writeInt(Self, buf[0..SIZE], self, .little);
    }

    pub inline fn fromBytes(buf: []const u8) Self {
        assert(SIZE == buf.len);
        return mem.readInt(Self, buf[0..SIZE], .little);
    }
};

pub const Read = struct {
    key: []const u8,

    pub const FLAG_TYPE: u8 = 0b00_000000;

    const Self = @This();

    pub fn flags(_: *const Self) u8 {
        return FLAG_TYPE;
    }

    pub fn size(self: *const Self) usize {
        return FLAGS_SIZE + Key.size(self.key);
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        assert(self.size() == buf.len);

        var cursor: usize = 0;

        buf[cursor] = self.flags();
        cursor += FLAGS_SIZE;

        Key.serialize(self.key, buf[cursor..]);
    }

    pub fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len <= FLAGS_SIZE) {
            return Error.Set.EOF;
        }

        var cursor: usize = 0;

        const flgs = buf[cursor];
        assert((flgs & TYPE_MASK) == Self.FLAG_TYPE);
        cursor += FLAGS_SIZE;

        return Self{
            .key = try Key.parse(buf[cursor..]),
        };
    }
};

pub const Write = struct {
    key: []const u8,
    val: ?[]const u8,

    const Self = @This();

    pub const FLAG_TYPE: u8 = 0b01_000000;

    pub fn flags(_: *const Self) u8 {
        return FLAG_TYPE;
    }

    pub fn size(self: *const Self) usize {
        return FLAGS_SIZE + Key.size(self.key) + Val.size(self.val);
    }

    pub fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < FLAGS_SIZE) {
            return Error.Set.EOF;
        }

        var cursor: usize = 0;

        const flgs = buf[cursor];
        assert((flgs & TYPE_MASK) == FLAG_TYPE);
        cursor += FLAGS_SIZE;

        if (buf.len <= cursor) {
            return Error.Set.EOF;
        }

        const k = try Key.parse(buf[cursor..]);
        cursor += Key.size(k);

        if (buf.len <= cursor) {
            return Error.Set.EOF;
        }

        const v = try Val.parse(buf[cursor..]);

        return Self{ .key = k, .val = v };
    }

    /// ## PERF
    /// - see how much this kinda thing helps or hurts
    pub fn fromBytes(buf: []const u8) Self {
        return Self.parse(buf) catch unreachable;
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        assert(self.size() == buf.len);

        var cursor: usize = 0;

        buf[cursor] = self.flags();
        cursor += FLAGS_SIZE;

        Key.serialize(self.key, buf[cursor .. cursor + Key.size(self.key)]);
        cursor += Key.size(self.key);

        Val.serialize(self.val, buf[cursor .. cursor + Val.size(self.val)]);
    }
};

/// unimplemented
pub const Macro = struct {};

pub const TxnCtrl = enum {
    Commit,
    Abort,

    const Self = @This();

    pub const SIZE = FLAGS_SIZE;

    pub const FLAG_TYPE: u8 = 0b11_000000;

    pub const FLAG_COMMIT: u8 = 0b00_00_0000;
    pub const FLAG_ABORT: u8 = 0b00_01_0000;

    pub const SUBTYPE_MASK: u8 = 0b00_11_0000;

    pub inline fn flags(self: *const Self) u8 {
        return FLAG_TYPE | switch (self.*) {
            .Commit => FLAG_COMMIT,
            .Abort => FLAG_ABORT,
        };
    }

    pub inline fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < FLAGS_SIZE) {
            return Error.Set.EOF;
        }

        const flgs = buf[0];
        assert((flgs & TYPE_MASK) == FLAG_TYPE);

        return switch (flgs & SUBTYPE_MASK) {
            FLAG_COMMIT => .Commit,
            FLAG_ABORT => .Abort,
            else => Error.Set.MALFORMED,
        };
    }

    pub inline fn serialize(self: *const Self, buf: []u8) void {
        assert(SIZE == buf.len);
        buf[0] = self.flags();
    }
};

pub const NET_HEADER_SIZE = TxnId.SIZE + FLAGS_SIZE;

pub const Request = struct {
    txn_id: u64,
    op: union(OpTag) {
        read: Read,
        write: Write,
        txn_ctrl: TxnCtrl,
    },

    const OpTag = enum {
        read,
        write,
        txn_ctrl,
    };

    const Self = @This();

    pub fn size(self: *const Self) usize {
        return TxnId.SIZE + switch (self.op) {
            .read => |read| read.size(),
            .write => |write| write.size(),
            .txn_ctrl => TxnCtrl.SIZE,
        };
    }

    pub fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < NET_HEADER_SIZE) {
            return Error.Set.EOF;
        }

        var cursor: usize = 0;

        const txn_id = try TxnId.parse(buf[cursor .. cursor + TxnId.SIZE]);
        cursor += TxnId.SIZE;
        const flags = buf[cursor];

        switch (flags & TYPE_MASK) {
            Read.FLAG_TYPE => {
                return Self{
                    .txn_id = txn_id,
                    .op = .{ .read = try Read.parse(buf[cursor..]) },
                };
            },
            Write.FLAG_TYPE => {
                return Self{
                    .txn_id = txn_id,
                    .op = .{ .write = try Write.parse(buf[cursor..]) },
                };
            },
            TxnCtrl.FLAG_TYPE => {
                return Self{
                    .txn_id = txn_id,
                    .op = .{ .txn_ctrl = try TxnCtrl.parse(buf[cursor..]) },
                };
            },
            else => return Error.Set.MALFORMED,
        }
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        assert(self.size() == buf.len);

        var cursor: usize = 0;

        TxnId.serialize(self.txn_id, buf[cursor .. cursor + TxnId.SIZE]);
        cursor += TxnId.SIZE;

        switch (self.op) {
            .read => |read| read.serialize(buf[cursor..]),
            .write => |write| write.serialize(buf[cursor..]),
            .txn_ctrl => |txn_ctrl| txn_ctrl.serialize(buf[cursor..]),
        }
    }
};

pub const Error = struct {
    err: Set,

    pub const Set = error{
        EOF,
        MALFORMED,
    };

    const Self = @This();

    pub const SIZE = @sizeOf(u8);

    const EOF_CODE: u8 = 1;
    const MALFORMED_CODE: u8 = 2;

    pub fn serialize(self: Self, buf: []u8) void {
        assert(SIZE == buf.len);
        buf[0] = switch (self.err) {
            Set.EOF => EOF_CODE,
            Set.MALFORMED => MALFORMED_CODE,
        };
    }

    pub fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < SIZE) {
            return Set.EOF;
        }

        return switch (buf[0]) {
            EOF_CODE => Self{ .err = Set.EOF },
            MALFORMED_CODE => Self{ .err = Set.MALFORMED },
            else => Set.MALFORMED,
        };
    }
};

pub const Response = struct {
    txn_id: u64,
    res: Error.Set!union(ResTag) {
        read: ?[]const u8,
        write,
        txn_ctrl,
    },

    const ResTag = enum {
        read,
        write,
        txn_ctrl,
    };

    const Self = @This();

    pub const RES_MASK: u8 = 0b0000000_1;

    pub const FLAG_OK: u8 = 0b0000000_0;
    pub const FLAG_ERR: u8 = 0b0000000_1;

    pub fn size(self: *const Self) usize {
        return NET_HEADER_SIZE + if (self.res) |res| switch (res) {
            .read => |val| Val.size(val),
            .write => 0,
            .txn_ctrl => 0,
        } else |_| Error.SIZE;
    }
    pub fn serialize(self: *const Self, buf: []u8) void {
        assert(self.size() == buf.len);

        var cursor: usize = 0;

        TxnId.serialize(self.txn_id, buf[cursor .. cursor + TxnId.SIZE]);
        cursor += TxnId.SIZE;

        if (self.res) |res| {
            buf[cursor] = FLAG_OK;

            switch (res) {
                .read => |val| {
                    buf[cursor] |= Read.FLAG_TYPE;
                    cursor += FLAGS_SIZE;
                    Val.serialize(val, buf[cursor..]);
                },
                .write => buf[cursor] |= Write.FLAG_TYPE,
                .txn_ctrl => buf[cursor] |= TxnCtrl.FLAG_TYPE,
            }
        } else |err| {
            buf[cursor] = FLAG_ERR;
            cursor += FLAGS_SIZE;
            const e = Error{ .err = err };
            e.serialize(buf[cursor..]);
        }
    }

    pub fn parse(buf: []const u8) Error.Set!Self {
        if (buf.len < NET_HEADER_SIZE) {
            return Error.Set.EOF;
        }

        var cursor: usize = 0;

        const txn_id = try TxnId.parse(buf[cursor .. cursor + TxnId.SIZE]);
        cursor += TxnId.SIZE;

        const flgs = buf[cursor];
        switch (flgs & RES_MASK) {
            FLAG_OK => {
                switch (flgs & TYPE_MASK) {
                    Read.FLAG_TYPE => {
                        if (buf.len <= cursor + FLAGS_SIZE) {
                            return Error.Set.EOF;
                        }
                        cursor += FLAGS_SIZE;

                        return Self{
                            .txn_id = txn_id,
                            .res = .{
                                .read = try Val.parse(buf[cursor..]),
                            },
                        };
                    },
                    Write.FLAG_TYPE => return Self{
                        .txn_id = txn_id,
                        .res = .write,
                    },
                    TxnCtrl.FLAG_TYPE => return Self{
                        .txn_id = txn_id,
                        .res = .txn_ctrl,
                    },
                    else => return Error.Set.MALFORMED,
                }
            },
            FLAG_ERR => {
                if (buf.len <= cursor + FLAGS_SIZE) {
                    return Error.Set.EOF;
                }
                cursor += FLAGS_SIZE;

                const e = try Error.parse(buf[cursor..]);

                return Self{
                    .txn_id = txn_id,
                    .res = e.err,
                };
            },
            else => return Error.Set.MALFORMED,
        }
    }
};

pub fn Iter(comptime T: type, comptime fallible: bool) type {
    if (fallible) {
        return struct {
            buf: []const u8,
            idx: usize,

            const Self = @This();

            pub fn fromBytes(buf: []const u8) Self {
                return Self{ .buf = buf, .idx = 0 };
            }

            pub fn next(self: *Self) Error.Set!?T {
                if (self.idx >= self.buf.len) {
                    return null;
                }
                if (T.parse(self.buf[self.idx..])) |t| {
                    self.idx += t.size();
                    return t;
                } else |err| switch (err) {
                    Error.Set.EOF => return null,
                    else => return err,
                }
            }
            pub fn reset(self: *Self) void {
                self.idx = 0;
            }
        };
    } else {
        return struct {
            buf: []const u8,
            idx: usize,

            const Self = @This();

            pub fn fromBytes(buf: []const u8) Self {
                return Self{ .buf = buf, .idx = 0 };
            }

            pub fn next(self: *Self) ?T {
                if (self.idx >= self.buf.len) {
                    return null;
                }
                const t = T.fromBytes(self.buf[self.idx..]);
                self.idx += t.size();
                return t;
            }

            pub fn nextBytes(self: *Self) ?[]const u8 {
                if (self.idx >= self.buf.len) {
                    return null;
                }
                const size = T.fromBytes(self.buf[self.idx..]).size();
                const out = self.buf[self.idx .. self.idx + size];
                self.idx += size;
                return out;
            }

            pub fn reset(self: *Self) void {
                self.idx = 0;
            }
        };
    }
}

pub const Page = struct {
    writes: []const u8,
    entries: []const u8,
    left_pid: u64,
    right_pid: u64,

    pub const ENTRIES_LEN_SIZE = @sizeOf(u64);
    pub const FOOTER_SIZE = ENTRIES_LEN_SIZE + PageId.SIZE + PageId.SIZE;

    const Self = @This();

    pub fn fromBytes(buf: []const u8) Self {
        var cursor = buf.len;

        const right_pid = PageId.parse(
            buf[cursor - PageId.SIZE .. cursor],
        ) catch unreachable;
        cursor -= PageId.SIZE;

        const left_pid = PageId.parse(
            buf[cursor - PageId.SIZE .. cursor],
        ) catch unreachable;
        cursor -= PageId.SIZE;

        const entries_len = mem.readInt(
            u64,
            buf[cursor - ENTRIES_LEN_SIZE .. cursor][0..ENTRIES_LEN_SIZE],
            .little,
        );
        cursor -= ENTRIES_LEN_SIZE;

        const entries = buf[cursor - entries_len .. cursor];
        cursor -= entries_len;

        const writes = buf[0..cursor];

        return Self{
            .writes = writes,
            .entries = entries,
            .left_pid = left_pid,
            .right_pid = right_pid,
        };
    }

    pub inline fn isLeaf(self: *const Self) bool {
        return self.left_pid != std.math.maxInt(u64);
    }

    pub fn searchLeaf(
        self: *const Self,
        target: []const u8,
        timestamp: u64,
    ) ?[]const u8 {
        assert(self.isLeaf());

        var commits = self.iterCommits();
        while (commits.next()) |commit| {
            if (timestamp < commit.timestamp) {
                continue;
            }
            if (mem.eql(u8, target, commit.write.key)) {
                return commit.write.val;
            }
        }

        var entries = self.iterEntries();
        while (entries.next()) |entry| {
            if (mem.eql(u8, target, entry.key)) {
                return entry.val;
            } else if (mem.lessThan(u8, target, entry.key)) {
                return null;
            }
        }

        return null;
    }

    pub fn iterCommits(self: *const Self) Iter(Commit, false) {
        assert(self.isLeaf());
        return Iter(Commit, false).fromBytes(self.writes);
    }
    pub fn iterWrites(self: *const Self) Iter(Write, false) {
        assert(!self.isLeaf());
        return Iter(Write, false).fromBytes(self.writes);
    }

    pub fn iterEntries(self: *const Self) Iter(Entry, false) {
        return Iter(Entry, false).fromBytes(self.entries);
    }
};

pub const Timestamp = struct {
    pub const SIZE = @sizeOf(u64);

    const Self = u64;

    pub inline fn fromBytes(buf: []const u8) Self {
        assert(buf.len >= SIZE);
        return mem.readInt(Self, buf[0..SIZE], .little);
    }

    pub inline fn serialize(self: Self, buf: []u8) void {
        assert(SIZE == buf.len);
        mem.writeInt(Self, buf[0..SIZE], self, .little);
    }
};

pub const Commit = struct {
    timestamp: u64,
    write: Write,

    const Self = @This();

    pub fn size(self: Self) usize {
        return Timestamp.SIZE + self.write.size();
    }

    pub fn fromBytes(buf: []const u8) Self {
        var cursor: usize = 0;

        const timestamp = Timestamp.fromBytes(buf[cursor..]);
        cursor += Timestamp.SIZE;

        return Self{
            .timestamp = timestamp,
            .write = Write.fromBytes(buf[cursor..]),
        };
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        assert(self.size() == buf.len);

        var cursor: usize = 0;

        Timestamp.serialize(
            self.timestamp,
            buf[cursor .. cursor + Timestamp.SIZE],
        );
        cursor += Timestamp.SIZE;

        self.write.serialize(buf[cursor..]);
    }
};

pub const Entry = struct {
    key: []const u8,
    val: []const u8,

    const Self = @This();

    pub fn size(self: *const Self) usize {
        return Key.size(self.key) + Val.LEN_SIZE + self.val.len;
    }

    pub fn fromBytes(buf: []const u8) Self {
        var cursor: usize = 0;

        const key = Key.parse(buf[cursor..]) catch unreachable;
        cursor += Key.size(key);

        const val_len = mem.readInt(
            u32,
            buf[cursor .. cursor + Val.LEN_SIZE][0..Val.LEN_SIZE],
            .little,
        );
        cursor += Val.LEN_SIZE;

        const val = buf[cursor .. cursor + val_len];

        return Self{
            .key = key,
            .val = val,
        };
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        assert(self.size() == buf.len);

        var cursor: usize = 0;

        Key.serialize(self.key, buf[cursor .. cursor + Key.size(self.key)]);
        cursor += Key.size(self.key);

        mem.writeInt(
            u32,
            buf[cursor .. cursor + Val.LEN_SIZE][0..Val.LEN_SIZE],
            @intCast(self.val.len),
            .little,
        );
        cursor += Val.LEN_SIZE;

        @memcpy(buf[cursor..], self.val);
    }
};

/// ## TODO
/// - adjust left/right pid stuff for range scans
///   (actually treat them as sibling pointers)
pub const PageBuilder = struct {
    chunks: std.ArrayListUnmanaged(PageChunk),

    commits: std.ArrayListUnmanaged(Commit),
    smos: std.ArrayListUnmanaged(Smop),
    entries: std.ArrayListUnmanaged(Entry),

    left_pid: u64,
    right_pid: u64,

    arena: heap.ArenaAllocator,

    const Self = @This();

    pub fn init(allocator: mem.Allocator) Self {
        return Self{
            .chunks = std.ArrayListUnmanaged(PageChunk).empty,

            .commits = std.ArrayListUnmanaged(Commit).empty,
            .smos = std.ArrayListUnmanaged(Smop).empty,
            .entries = std.ArrayListUnmanaged(Entry).empty,

            .left_pid = 0,
            .right_pid = 0,

            .arena = heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn appendChunk(self: *Self, chunk: PageChunk) void {
        self.chunks.append(self.arena.allocator(), chunk) catch unreachable;
    }

    pub fn clear(self: *Self) void {
        assert(self.arena.reset(.retain_capacity));

        self.commits = std.ArrayListUnmanaged(Commit).empty;
        self.entries = std.ArrayListUnmanaged(Entry).empty;

        self.left_pid = 0;
        self.right_pid = 0;
    }

    pub fn entriesSize(self: *const Self) usize {
        var total: usize = 0;
        for (self.entries.items) |entry| total += entry.size();
        return total;
    }
    pub fn commitsSize(self: *const Self) usize {
        var total: usize = 0;
        for (self.commits.items) |commit| total += commit.size();
        return total;
    }
    pub fn size(self: *const Self) usize {
        return self.commitsSize() + self.entriesSize() + Page.FOOTER_SIZE;
    }

    pub fn compact(self: *Self, page: Page, timestamp: ?u64) void {
        self.clear();

        self.left_pid = page.left_pid;
        self.right_pid = page.right_pid;

        var entries = page.iterEntries();
        while (entries.next()) |entry| {
            self.entries.append(
                self.arena.allocator(),
                entry,
            ) catch unreachable;
        }

        if (page.isLeaf()) {
            var commits = page.iterCommits();
            while (commits.next()) |commit| {
                if (timestamp.? < commit.timestamp) {
                    // start actual compaction
                    self.applyWrite(commit.write);
                    break;
                } else {
                    self.commits.append(
                        self.arena.allocator(),
                        commit,
                    ) catch unreachable;
                }
            }
            while (commits.next()) |commit| {
                self.applyWrite(commit.write);
            }
        } else {
            var writes = page.iterWrites();
            while (writes.next()) |write| {
                self.applyWrite(write);
            }
        }
    }

    pub fn applyWrite(self: *Self, write: Write) void {
        if (write.val) |val| {
            for (self.entries.items, 0..) |*entry, i| {
                if (mem.eql(u8, entry.key, write.key)) {
                    entry.val = val;
                    return;
                } else if (mem.lessThan(u8, write.key, entry.key)) {
                    self.entries.insert(
                        self.arena.allocator(),
                        i,
                        Entry{ .key = write.key, .val = val },
                    ) catch unreachable;
                    return;
                }
            }
            self.entries.append(
                self.arena.allocator(),
                Entry{ .key = write.key, .val = val },
            ) catch unreachable;
        } else {
            for (self.entries.items, 0..) |entry, i| {
                if (mem.eql(u8, entry.key, write.key)) {
                    _ = self.entries.orderedRemove(i);
                    return;
                }
            }
        }
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        assert(self.size() == buf.len);

        var cursor: usize = 0;

        for (self.commits.items) |commit| {
            commit.serialize(buf[cursor .. cursor + commit.size()]);
            cursor += commit.size();
        }

        for (self.entries.items) |entry| {
            entry.serialize(buf[cursor .. cursor + entry.size()]);
            cursor += entry.size();
        }

        mem.writeInt(
            u64,
            buf[cursor .. cursor + Page.ENTRIES_LEN_SIZE][0..Page.ENTRIES_LEN_SIZE],
            self.entriesSize(),
            .little,
        );
        cursor += Page.ENTRIES_LEN_SIZE;

        PageId.serialize(self.left_pid, buf[cursor .. cursor + PageId.SIZE]);
        cursor += PageId.SIZE;

        PageId.serialize(self.right_pid, buf[cursor .. cursor + PageId.SIZE]);
        cursor += PageId.SIZE;

        assert(cursor == buf.len);
    }

    pub fn splitLeaf(self: *Self, allocator: mem.Allocator) Self {
        assert(self.left_pid != std.math.maxInt(u64));

        var to = Self.init(allocator);
        to.left_pid = self.left_pid;

        const middle_i = self.entries.items.len / 2;
        for (0..middle_i + 1) |i| {
            to.entries.append(
                to.arena.allocator(),
                self.entries.items[i],
            ) catch unreachable;
        }
        for (0..middle_i + 1) |_| {
            _ = self.entries.orderedRemove(0);
        }

        const middle_key = to.entries.getLast().key;
        var to_remove = std.ArrayListUnmanaged(usize).empty;
        defer to_remove.deinit(self.arena.allocator());
        for (self.commits.items, 0..) |commit, i| {
            const goesLeft = mem.order(
                u8,
                commit.write.key,
                middle_key,
            ) != .gt;
            if (goesLeft) {
                to.commits.append(
                    to.arena.allocator(),
                    commit,
                ) catch unreachable;
                to_remove.append(self.arena.allocator(), i) catch unreachable;
            }
        }
        for (to_remove.items, 0..) |i, j| {
            _ = self.commits.orderedRemove(i - j);
        }

        return to;
    }

    pub fn splitInner(self: *Self, allocator: mem.Allocator) Self {
        assert(self.left_pid == std.math.maxInt(u64));
        assert(self.commits.items.len == 0);

        var to = Self.init(allocator);
        to.left_pid = self.left_pid;

        const middle_i = self.entries.items.len / 2;
        for (0..middle_i + 1) |i| {
            to.entries.append(
                to.arena.allocator(),
                self.entries.items[i],
            ) catch unreachable;
        }
        for (0..middle_i + 1) |_| {
            _ = self.entries.orderedRemove(0);
        }

        return to;
    }
};

/// format:
/// ```
/// [ HEADER                            ]
///     [ type            (u8) ]
///     [ len            (u64) ]
///     [ next_off (u64) ]
/// [ COMMITS | SMOPS | ENTRIES (bytes) ]
/// ```
///
/// ## TODO
/// - figure out what to do with next_off when we already have the type
///   (for now we're just putting it there every time, 0 for entries chunk)
pub fn PageChunk(comptime pt: page_type) type {
    return union(Tag) {
        const Self = @This();
        pub const Tag = enum(u8) { commits, smops, entries };
        pub const HEADER_SIZE = @sizeOf(u8) + (@sizeOf(u64) * 2);
        const Entries = switch (pt) {
            .leaf => LeafEntries,
            .inner => InnerEntries,
        };

        commits: struct { commits: Iter(Commit, false), next: u64 },
        smops: struct { smops: Iter(Smop, false), next: u64 },
        entries: Entries,

        pub fn size(self: *const Self) usize {
            return HEADER_SIZE + switch (self.*) {
                .commits => |c| c.commits.buf.len,
                .smops => |s| s.smops.buf.len,
                .entries => |e| e.size(),
            };
        }

        pub fn serialize(self: *const Self, buf: []u8) void {
            debug.assert(self.size() == buf.len);
            var cursor: usize = 0;
            switch (self.*) {
                .commits => |c| {
                    buf[cursor] = @intFromEnum(Tag.commits);
                    cursor += @sizeOf(u8);
                    mem.writeInt(
                        u64,
                        buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                        c.commits.buf.len,
                        .little,
                    );
                    cursor += @sizeOf(u64);
                    mem.writeInt(
                        u64,
                        buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                        c.next,
                        .little,
                    );
                    cursor += @sizeOf(u64);
                    @memcpy(buf[cursor..], c.commits.buf);
                },
                .smops => |s| {
                    buf[cursor] = @intFromEnum(Tag.smops);
                    cursor += @sizeOf(u8);
                    mem.writeInt(
                        u64,
                        buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                        s.smops.buf.len,
                        .little,
                    );
                    cursor += @sizeOf(u64);
                    mem.writeInt(
                        u64,
                        buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                        s.next,
                        .little,
                    );
                    cursor += @sizeOf(u64);
                    @memcpy(buf[cursor..], s.smops.buf);
                },
                .entries => |e| {
                    buf[cursor] = @intFromEnum(Tag.entries);
                    cursor += @sizeOf(u8);
                    mem.writeInt(
                        u64,
                        buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                        e.size(),
                        .little,
                    );
                    cursor += @sizeOf(u64);
                    mem.writeInt(
                        u64,
                        buf[cursor .. cursor + @sizeOf(u64)][0..@sizeOf(u64)],
                        0,
                        .little,
                    );
                    e.serialize(buf[cursor..]);
                },
            }
        }

        pub fn fromBytes(buf: []const u8) Self {
            var cursor: usize = 0;

            const t: Tag = @enumFromInt(buf[cursor]);
            cursor += @sizeOf(u8);

            const len = mem.bytesToValue(
                u64,
                buf[cursor .. cursor + @sizeOf(u64)],
            );
            cursor += @sizeOf(u64);

            const next_off = mem.bytesToValue(
                u64,
                buf[cursor .. cursor + @sizeOf(u64)],
            );
            cursor += @sizeOf(u64);

            switch (t) {
                Tag.commits => {
                    return Self{
                        .commits = .{
                            .commits = Iter(Commit, false).fromBytes(
                                buf[cursor .. cursor + len],
                            ),
                            .next = next_off,
                        },
                    };
                },
                Tag.smops => {
                    return Self{
                        .smops = .{
                            .smops = Iter(Smop, false).fromBytes(
                                buf[cursor .. cursor + len],
                            ),
                            .next = next_off,
                        },
                    };
                },
                Tag.entries => {
                    return Self{
                        .entries = Entries.fromBytes(
                            buf[cursor .. cursor + len],
                        ),
                    };
                },
            }
        }
    };
}

pub const page_type = enum {
    inner,
    leaf,
};

/// a [S]tructure [M]odification [OP]eration
pub const Smop = struct {
    const Self = @This();

    pid: u64,
    gt_key: []const u8,
    lte_key: []const u8,

    pub fn size(self: *const Self) usize {
        return PageId.SIZE +
            (Key.LEN_SIZE * 2) +
            self.gt_key.len +
            self.lte_key.len;
    }
    pub fn fromBytes(buf: []const u8) Self {
        var cursor: usize = 0;
        const pid = PageId.fromBytes(buf[cursor .. cursor + PageId.SIZE]);
        cursor += PageId.SIZE;
        const gt_key = Key.parse(buf[cursor..]) catch unreachable;
        cursor += Key.size(gt_key);
        const lte_key = Key.parse(buf[cursor..]) catch unreachable;
        return Self{
            .pid = pid,
            .gt_key = gt_key,
            .lte_key = lte_key,
        };
    }
    pub fn serialize(self: *const Self, buf: []u8) void {
        debug.assert(self.size() == buf.len);
        var cursor: usize = 0;
        PageId.serialize(self.pid, buf[cursor .. cursor + PageId.SIZE]);
        cursor += PageId.SIZE;
        Key.serialize(
            self.gt_key,
            buf[cursor .. cursor + Key.size(self.gt_key)],
        );
        cursor += Key.size(self.gt_key);
        Key.serialize(self.lte_key, buf[cursor..]);
    }
};

/// format:
/// ```
/// [ num          (u16) ]
/// [ pids      ...(u64) ]
/// [ key offs  ...(u32) ]
/// [ key lens  ...(u16) ]
/// [ keys    ...(bytes) ]
/// ```
/// pids, offsets, lengths, and keys themselves are in key-sorted order
pub const InnerEntries = struct {
    const Self = @This();

    pids: []align(1) const u64,

    key_offs: []align(1) const u32,
    key_lens: []align(1) const u16,
    keys: []const u8,

    pub fn size(self: *const Self) usize {
        return @sizeOf(u16) +
            (self.pids.len * @sizeOf(u64)) +
            (self.key_offs.len * @sizeOf(u32)) +
            (self.key_lens.len * @sizeOf(u16)) +
            self.keys.len;
    }

    pub fn serialize(self: *const Self, buf: []u8) void {
        var cursor: usize = 0;
        mem.writeInt(
            u16,
            buf[cursor .. cursor + @sizeOf(u16)][0..@sizeOf(u16)],
            @intCast(self.pids.len - 1),
            .little,
        );
        cursor += @sizeOf(u16);
        @memcpy(
            buf[cursor .. cursor + (self.pids.len * @sizeOf(u64))],
            mem.sliceAsBytes(self.pids),
        );
        cursor += (self.pids.len * @sizeOf(u64));
        @memcpy(
            buf[cursor .. cursor + (self.key_offs.len * @sizeOf(u32))],
            mem.sliceAsBytes(self.key_offs),
        );
        cursor += (self.key_offs.len * @sizeOf(u32));
        @memcpy(
            buf[cursor .. cursor + (self.key_lens.len * @sizeOf(u16))],
            mem.sliceAsBytes(self.key_lens),
        );
        cursor += (self.key_lens.len * @sizeOf(u16));
        @memcpy(buf[cursor..], self.keys);
    }

    /// expects exact size buffer
    pub fn fromBytes(buf: []const u8) Self {
        var cursor: usize = 0;

        const num = mem.bytesToValue(
            u16,
            buf[cursor .. cursor + @sizeOf(u16)],
        );
        cursor += @sizeOf(u16);

        const pids = mem.bytesAsSlice(
            u64,
            buf[cursor .. cursor + (@sizeOf(u64) * (num + 1))],
        );
        cursor += @sizeOf(u64) * (num + 1);

        const key_offs = mem.bytesAsSlice(
            u32,
            buf[cursor .. cursor + (@sizeOf(u32) * num)],
        );
        cursor += @sizeOf(u32) * num;

        const key_lens = mem.bytesAsSlice(
            u16,
            buf[cursor .. cursor + (@sizeOf(u16) * num)],
        );
        cursor += @sizeOf(u16) * num;

        const keys = buf[cursor..];

        return Self{
            .pids = pids,
            .key_offs = key_offs,
            .key_lens = key_lens,
            .keys = keys,
        };
    }

    pub fn search(self: *const Self, target: []const u8) u64 {
        for (0..self.key_offs.len) |i| {
            const off = self.key_offs[i];
            const len = self.key_lens[i];
            const key = self.keys[off .. off + len];
            switch (mem.order(u8, target, key)) {
                .lt, .eq => {
                    return self.pids[i];
                },
                .gt => {},
            }
        }
        return self.pids[self.pids.len - 1];
    }
};

/// format:
/// ```
/// [ num          (u16) ]
/// [ offs      ...(u32) ]
/// [ key lens  ...(u16) ]
/// [ val lens  ...(u32) ]
/// [ entries ...(bytes) ]
/// ```
pub const LeafEntries = struct {
    const Self = @This();

    key_offs: []align(1) const u32,
    key_lens: []align(1) const u16,
    keys: []const u8,

    pub fn fromBytes(_: []const u8) Self {}
};
