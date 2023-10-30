use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{stdin, stdout, StdoutLock, Write},
};
use ulid::Ulid;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
    Broadcast {
        message: i32,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<i32>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}
struct EchoNode {
    id: usize,
}

impl EchoNode {
    pub fn step(
        &mut self,
        input: Message,
        output: &mut StdoutLock,
        rpc: &mut HashSet<i32>,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            Payload::EchoOk { .. } => {}
            Payload::Init { .. } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("asd")?;
                self.id += 1;
            }
            Payload::InitOk { .. } => bail!("not expecting init_ok from other peers"),
            Payload::Generate { .. } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk {
                            guid: Ulid::new().to_string(),
                        },
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            Payload::GenerateOk { .. } => bail!("not expecting generate_ok from other peers"),
            Payload::Broadcast { message } => {
                rpc.insert(message);
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::BroadcastOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            Payload::BroadcastOk { .. } => todo!(),
            Payload::Read => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::ReadOk {
                            messages: rpc.clone(),
                        },
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            Payload::ReadOk { .. } => bail!("not expecting read_ok from other peers"),
            Payload::Topology { topology } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::TopologyOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            Payload::TopologyOk => bail!("not expecting topology_ok"),
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = stdin().lock();
    let mut stdout = stdout().lock();
    let mut rpc_data = HashSet::new();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut state = EchoNode { id: 0 };
    for input in inputs {
        let input = input.context("STDIN cannot be deserialized")?;
        state
            .step(input, &mut stdout, &mut rpc_data)
            .context("Node step function failed")?;
    }

    Ok(())
}
