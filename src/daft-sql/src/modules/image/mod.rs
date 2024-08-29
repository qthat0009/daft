use crate::functions::SQLFunctions;

use super::SQLModule;

pub mod decode;
pub mod encode;

pub struct SQLModuleImage;

impl SQLModule for SQLModuleImage {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("image_decode", decode::SQLImageDecode {});
        parent.add_fn("image_encode", encode::SQLImageEncode {});
    }
}
