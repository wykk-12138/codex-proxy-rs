//! 已完整读取的响应正文交付
//!
//! notes 写响应与本地 history 结果都已离开上游惰性读取，按单帧原始字节交付
//! 关闭或丢弃不保留后台任务，也不改写任何字段

use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::engine::middleware::{
    MiddlewareBody, MiddlewareError, MiddlewareFrame, MiddlewareFraming,
};

/// 单帧正文字面；每个实例只交付一次
pub(crate) struct BufferedBody {
    bytes: Option<Bytes>,
}

impl BufferedBody {
    pub(crate) const fn new(bytes: Bytes) -> Self {
        Self { bytes: Some(bytes) }
    }
}

impl MiddlewareBody for BufferedBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            Ok(self
                .bytes
                .take()
                .map(|bytes| MiddlewareFrame::new(bytes, MiddlewareFraming::RawBytes, true)))
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}
