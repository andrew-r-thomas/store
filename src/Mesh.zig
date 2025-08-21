const Mesh = @This();

// IMPORTS ////////////////////////////////////////////////////////////////////
const std = @import("std");
const atomic = std.atomic;
const mem = std.mem;
const debug = std.debug;
const print = debug.print;
const testing = std.testing;
///////////////////////////////////////////////////////////////////////////////

// FIELDS /////////////////////////////////////////////////////////////////////
push_cursors: []atomic.Value(usize),
pop_cursors: []atomic.Value(usize),
buf: []Msg,

num_endpoints: usize,
queue_depth: usize,
///////////////////////////////////////////////////////////////////////////////

// FUNCTIONS //////////////////////////////////////////////////////////////////
pub fn init(
    num_endpoints: usize,
    queue_depth: usize,
    allocator: mem.Allocator,
) !Mesh {
    // assert queue depth is a power of 2
    debug.assert((queue_depth & (queue_depth - 1)) == 0);

    // 2 queues between each endpoint (to and from)
    // no queues between the same endpoint
    //
    //    [a][b][c]
    // [a] -  1  1
    // [b] 1  -  1
    // [c] 1  1  -
    const num_queues = (num_endpoints * num_endpoints) - num_endpoints;

    // load up cursors with 0s
    var push_cursors = try allocator.alloc(atomic.Value(usize), num_queues);
    var pop_cursors = try allocator.alloc(atomic.Value(usize), num_queues);
    for (0..num_queues) |i| {
        push_cursors[i] = atomic.Value(usize).init(0);
        pop_cursors[i] = atomic.Value(usize).init(0);
    }

    return Mesh{
        .push_cursors = push_cursors,
        .pop_cursors = pop_cursors,
        .buf = try allocator.alloc(Msg, queue_depth * num_queues),

        .num_endpoints = num_endpoints,
        .queue_depth = queue_depth,
    };
}
pub fn deinit(self: *Mesh, allocator: mem.Allocator) void {
    allocator.free(self.push_cursors);
    allocator.free(self.pop_cursors);
    allocator.free(self.buf);
}

pub fn send(self: *const Mesh, msg: Msg, to: usize, from: usize) Error!void {
    const slot = ((self.num_endpoints - 1) * to) +
        (from - @as(usize, @intFromBool(from > to)));

    const push = &self.push_cursors[slot];
    const pop = &self.pop_cursors[slot];
    const push_off = push.load(.unordered);
    const pop_off = pop.load(.acquire);

    if (push_off - pop_off == self.queue_depth) return Error.FULL;

    const i = (slot * self.queue_depth) + (push_off & (self.queue_depth - 1));
    self.buf[i] = msg;

    push.store(push_off + 1, .release);
}
pub fn poll(self: *const Mesh, to: usize, closure: anytype) void {
    const start_slot = (self.num_endpoints - 1) * to;
    for (0..(self.num_endpoints - 1)) |slot_off| {
        const slot = start_slot + slot_off;
        const from = slot_off + @as(usize, @intFromBool(slot_off >= to));

        const push = &self.push_cursors[slot];
        const pop = &self.pop_cursors[slot];
        const push_off = push.load(.acquire);
        const pop_off = pop.load(.unordered);

        const start_q = pop_off & (self.queue_depth - 1);
        const total_len = push_off - pop_off;
        const first_len = @min(total_len, self.queue_depth - start_q);
        const last_len = total_len - first_len;

        const buf_off = slot * self.queue_depth;

        const used = closure.call(
            from,
            self.buf[buf_off + start_q .. buf_off + start_q + first_len],
            self.buf[buf_off .. buf_off + last_len],
        );

        pop.store(pop_off + used, .release);
    }
}
///////////////////////////////////////////////////////////////////////////////

// MSG ////////////////////////////////////////////////////////////////////////
pub const MsgTag = enum {
    newConn,
    txnStart,
    commitReq,
    commitResp,
    writeReq,
    writeResp,
};
pub const Msg = union(MsgTag) {
    newConn: u32,
    txnStart: u64,
    commitReq: struct {},
    commitResp: struct {},
    writeReq: struct {},
    writeResp: struct {},
};
///////////////////////////////////////////////////////////////////////////////

// ERROR //////////////////////////////////////////////////////////////////////
pub const Error = error{
    FULL,
};
///////////////////////////////////////////////////////////////////////////////

// TESTS //////////////////////////////////////////////////////////////////////
test "scratch" {
    const Thread = std.Thread;
    const num_endpoints = 4;
    var mesh = try Mesh.init(num_endpoints, 8, testing.allocator);
    defer mesh.deinit(testing.allocator);

    var endpoints: [num_endpoints]Thread = undefined;
    for (0..num_endpoints) |id| {
        endpoints[id] = try Thread.spawn(
            Thread.SpawnConfig{
                .stack_size = 1024,
                .allocator = testing.allocator,
            },
            runCounter,
            .{ id, &mesh },
        );
    }

    for (endpoints) |e| {
        e.join();
    }
}

fn runCounter(id: usize, mesh: *const Mesh) !void {
    const prevs = try testing.allocator.alloc(u64, mesh.num_endpoints);
    defer testing.allocator.free(prevs);
    for (prevs) |*p| {
        p.* = 0;
    }
    for (1..1025) |i| {
        var e: usize = 0;
        while (e < mesh.num_endpoints) {
            if (e != id) {
                mesh.send(.{ .txnStart = i }, e, id) catch {
                    print("to {d} from {d} full on {d}\n", .{ e, id, i });
                    continue;
                };
            }
            e += 1;
        }
        mesh.poll(id, counterPoll{ .prevs = prevs });
    }
}

const counterPoll = struct {
    prevs: []u64,
    pub fn call(
        self: *const @This(),
        from: usize,
        first: []const Msg,
        last: []const Msg,
    ) usize {
        for (first) |msg| {
            const i = switch (msg) {
                .txnStart => |n| n,
                else => unreachable,
            };
            debug.assert(self.prevs[from] == i - 1);
            self.prevs[from] = i;
        }
        for (last) |msg| {
            const i = switch (msg) {
                .txnStart => |n| n,
                else => unreachable,
            };
            debug.assert(self.prevs[from] == i - 1);
            self.prevs[from] = i;
        }

        return first.len + last.len;
    }
};
///////////////////////////////////////////////////////////////////////////////
