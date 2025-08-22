const std = @import("std");
const debug = std.debug;
const print = debug.print;
const heap = std.heap;

const store = @import("store_lib");
const Shard = store.Shard;
const Mesh = store.Mesh;

pub fn main() !void {
    var gpa = heap.GeneralPurposeAllocator(.{}).init;
    var mesh = try Mesh.init(1, 0, gpa.allocator());
    var shard = Shard.init(
        &mesh,
        gpa.allocator(),
    );
    defer shard.deinit();

    while (true) {
        shard.pump();
    }
}
