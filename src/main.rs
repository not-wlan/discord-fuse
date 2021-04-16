use anyhow::Result;
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    ReplyWrite, Request,
};
use libc::{EIO, ENOENT};
use log::debug;

use fuse::consts::FOPEN_DIRECT_IO;
use serenity::{
    http::{client::Http, GuildPagination},
    model::{
        channel::ChannelType,
        id::{ChannelId, GuildId},
    },
};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    time::{Duration, UNIX_EPOCH},
};
use tokio::runtime::Runtime;

const TTL: Duration = Duration::from_secs(1);

// Map inodes to DiscordFiles
type FileTree = BTreeMap<u64, DiscordFile>;

struct DiscordFS<'a> {
    discord: &'a Http,
    files: FileTree,
}

#[derive(Debug, Copy, Clone)]
enum DiscordFileType {
    Guild,
    ChannelFile(u64, u64),
}

#[derive(Debug, Clone)]
struct DiscordFile {
    filename: String,
    ty: DiscordFileType,
    parent: u64,
    attr: FileAttr,
}

const ROOT_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

impl<'a> Filesystem for DiscordFS<'a> {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent: {}, name: {:?})", parent, name);

        let name = name.to_str().expect("Bad name!");
        let result = self
            .files
            .iter()
            .filter(|&(_, v)| v.parent == parent)
            .find(|&(_, file)| file.filename.eq(name));

        if let Some((_, file)) = result {
            reply.entry(&TTL, &file.attr, 0);
        } else {
            debug!(
                "ERROR: lookup(parent: {}, name: {:?}): ENOENT",
                parent, name
            );
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino: {})", ino);

        if ino == 1 {
            reply.attr(&TTL, &ROOT_DIR_ATTR);
            return;
        }

        if let Some(file) = self.files.get(&ino) {
            reply.attr(&TTL, &file.attr);
        } else {
            debug!("ERROR: getattr(ino: {}): ENOENT", ino);
            reply.error(ENOENT);
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("open(ino: {}, flags: {:#X})", ino, flags);
        // This is necessary so writes aren't split
        reply.opened(0, FOPEN_DIRECT_IO);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        reply: ReplyData,
    ) {
        debug!("read(ino: {}, offset: {})", ino, offset);

        if let Some(DiscordFileType::ChannelFile(_, id)) = self.files.get(&ino).map(|file| file.ty)
        {
            let mut v = Runtime::new()
                .unwrap()
                .block_on(self.discord.get_messages(id, ""));

            if let Ok(channel) = v.as_mut() {
                channel.reverse();

                let msgs = channel
                    .iter()
                    .map(|message| {
                        let attachments = message
                            .attachments
                            .iter()
                            .map(|att| format!("{} ", att.url))
                            .collect::<String>();
                        format!(
                            "{}#{:04}: {}\n",
                            message.author.name,
                            message.author.discriminator,
                            if message.attachments.is_empty() {
                                message.content.to_owned()
                            } else {
                                format!("{} {}", message.content, attachments)
                            }
                        )
                    })
                    .collect::<String>();
                if offset.is_positive() && (offset as usize) >= msgs.len() {
                    reply.data(&[]);
                } else {
                    reply.data(&msgs.as_bytes()[offset as usize..]);
                }
                return;
            }
        }
        reply.error(ENOENT);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        debug!("write(ino: {}, data: {:?})", ino, data);

        if let Some(file) = self.files.get(&ino) {
            if let DiscordFileType::ChannelFile(_, channel) = file.ty {
                let text = String::from_utf8_lossy(data);
                let res = Runtime::new()
                    .unwrap()
                    .block_on(ChannelId(channel).say(&self.discord, &text));

                match res {
                    Ok(_) => reply.written(text.as_bytes().len() as u32),
                    _ => reply.error(EIO),
                }
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir(ino: {}, offset: {})", ino, offset);
        if self.files.contains_key(&ino) || ino == 1 {
            let files = self
                .files
                .iter()
                .filter(|&(_, v)| v.parent == ino)
                .collect::<Vec<_>>();

            let entries = vec![
                (1, FileType::Directory, "."),
                (1, FileType::Directory, ".."),
            ];

            entries
                .into_iter()
                .chain(files.iter().map(|&(ino, file)| {
                    (
                        *ino,
                        match file.ty {
                            DiscordFileType::Guild => FileType::Directory,
                            DiscordFileType::ChannelFile(_, _) => FileType::RegularFile,
                        },
                        file.filename.as_str(),
                    )
                }))
                .enumerate()
                .skip(offset as usize)
                .for_each(|(i, (ino, ty, name))| {
                    reply.add(ino, (i + 1) as i64, ty, name);
                });

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }
}

fn unique_name(base: &str, known_names: &[String]) -> String {
    let mut name = base.to_string();

    if known_names.contains(&name) {
        for i in 1.. {
            name = format!("{} ({})", base, i);

            if !known_names.contains(&name) {
                break;
            }
        }
    }

    name
}

async fn build_file_tree(client: &Http) -> Result<FileTree> {
    let guilds = client
        .get_guilds(&GuildPagination::After(GuildId(0)), 100)
        .await?;

    let mut files = FileTree::new();
    let mut guild_names = vec![];

    for guild in &guilds {
        let name = unique_name(&guild.name, &guild_names);
        guild_names.push(name.clone());

        files.insert(
            guild.id.0,
            DiscordFile {
                filename: name,
                ty: DiscordFileType::Guild,
                parent: 1,
                attr: FileAttr {
                    ino: guild.id.0,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o555,
                    nlink: 2,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                },
            },
        );

        let channels = guild.id.channels(&client).await?;

        let mut channel_names = vec![];

        for (key, value) in &channels {
            if value.kind != ChannelType::Text {
                continue;
            }

            let name = unique_name(&value.name, &channel_names);
            channel_names.push(name.clone());

            files.insert(
                key.0,
                DiscordFile {
                    filename: name,
                    ty: DiscordFileType::ChannelFile(guild.id.0, key.0),
                    parent: guild.id.0,
                    attr: FileAttr {
                        ino: key.0,
                        size: u32::MAX as u64,
                        blocks: 0,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::RegularFile,
                        perm: 0o644,
                        nlink: 1,
                        uid: 501,
                        gid: 20,
                        rdev: 0,
                        flags: 0,
                    },
                },
            );
        }
    }
    Ok(files)
}

fn main() -> Result<()> {
    env_logger::init();
    let token = std::env::var("DISCORD_TOKEN").expect("token");
    let client = Http::new_with_token(&token);

    let files = Runtime::new().unwrap().block_on(build_file_tree(&client))?;

    let mountpoint = std::env::args_os().nth(1).unwrap();
    let options = ["-o", "fsname=discordfuse"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    fuse::mount(
        DiscordFS {
            discord: &client,
            files,
        },
        &mountpoint,
        &options,
    )
    .unwrap();

    Ok(())
}
