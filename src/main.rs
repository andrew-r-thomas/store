use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
};

const PORT: u32 = 42069;
const SHARDS: usize = 4; // for now

#[derive(Clone)]
struct ShardHandle {
    send: SyncSender<ShardMessage>,
}
enum ShardMessage {
    NewConn { stream: TcpStream },
}

fn main() {
    let (shards, recvs): (Vec<ShardHandle>, Vec<Receiver<ShardMessage>>) = (0..SHARDS)
        .map(|_| {
            let (send, recv) = mpsc::sync_channel(16);
            (ShardHandle { send }, recv)
        })
        .collect();

    for recv in recvs {
        let shards = shards.clone();
        thread::spawn(move || {
            let mut conns = Vec::new();
            loop {
                if let Ok(msg) = recv.try_recv() {
                    match msg {
                        ShardMessage::NewConn { stream } => conns.push(stream),
                    }
                }

                let mut read_buf = vec![0; 1024];
                for conn in &mut conns {
                    conn.read(&mut read_buf).unwrap();
                }
            }
        });
    }

    let listener = TcpListener::bind(format!("localhost:{PORT}")).unwrap();
    let mut next_shard = 0;
    for s in listener.incoming() {
        if let Ok(stream) = s {
            shards[next_shard]
                .send
                .send(ShardMessage::NewConn { stream })
                .unwrap();

            if next_shard == SHARDS - 1 {
                next_shard = 0;
            } else {
                next_shard += 1;
            }
        }
    }
}
