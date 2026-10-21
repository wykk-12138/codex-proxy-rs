//! 有界流式归档写入：压缩后限额与摘要覆盖实际上传的字节

use async_compression::tokio::write::GzipEncoder;
use sha2::{Digest as _, Sha256};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncWrite, AsyncWriteExt as _, BufWriter};

struct ArchiveSink {
    file: BufWriter<tokio::fs::File>,
    maximum: u64,
    size: u64,
    hash: Sha256,
}

impl AsyncWrite for ArchiveSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if bytes.len() as u64 > self.maximum.saturating_sub(self.size) {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::FileTooLarge)));
        }
        match Pin::new(&mut self.file).poll_write(cx, bytes) {
            Poll::Ready(Ok(count)) => {
                self.size += count as u64;
                self.hash.update(&bytes[..count]);
                Poll::Ready(Ok(count))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

pub(super) struct ArchiveWriter(GzipEncoder<ArchiveSink>);

impl ArchiveWriter {
    pub fn new(file: std::fs::File, maximum: u64) -> Self {
        Self(GzipEncoder::new(ArchiveSink {
            file: BufWriter::new(tokio::fs::File::from_std(file)),
            maximum,
            size: 0,
            hash: Sha256::new(),
        }))
    }

    pub async fn finish(mut self) -> io::Result<(u64, String)> {
        self.shutdown().await?;
        let sink = self.0.into_inner();
        sink.file.get_ref().sync_all().await?;
        Ok((sink.size, hex::encode(sink.hash.finalize())))
    }
}

impl AsyncWrite for ArchiveWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
