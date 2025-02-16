mod error;

pub use self::error::{Error, Result};

#[cfg_attr(feature = "with-rpc", derive(rpc_router::RpcResource))]
#[derive(Clone, Debug)]
pub struct Ctx {
    user_id: i64,
    conv_id: Option<i64>,
}

impl Ctx {
    pub fn root_ctx() -> Self {
        Ctx {
            user_id: 0,
            conv_id: None,
        }
    }

    pub fn new(user_id: i64) -> Result<Self> {
        if user_id == 0 {
            Err(Error::CtxCannotNewRootCtx)
        } else {
            Ok(Self {
                user_id,
                conv_id: None,
            })
        }
    }

    pub fn add_conv_id(&self, conv_id: i64) -> Ctx {
        let mut ctx = self.clone();
        ctx.conv_id = Some(conv_id);
        ctx
    }
}

impl Ctx {
    pub fn user_id(&self) -> i64 {
        self.user_id
    }

    pub fn conv_id(&self) -> Option<i64> {
        self.conv_id
    }
}
