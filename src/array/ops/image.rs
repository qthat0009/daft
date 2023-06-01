use std::borrow::Cow;
use std::vec;

use image::ImageBuffer;

use crate::datatypes::logical::ImageArray;
use crate::datatypes::{DataType, Field, ImageMode, StructArray};
use crate::error::DaftResult;
use crate::with_match_numeric_daft_types;
use image::{Luma, LumaA, Rgb, Rgba};

use super::as_arrow::AsArrow;
use num_traits::FromPrimitive;

use std::ops::Deref;

#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug)]
enum DaftImageBuffer<'a> {
    L(ImageBuffer<Luma<u8>, Cow<'a, [u8]>>),
    LA(ImageBuffer<LumaA<u8>, Cow<'a, [u8]>>),
    RGB(ImageBuffer<Rgb<u8>, Cow<'a, [u8]>>),
    RGBA(ImageBuffer<Rgba<u8>, Cow<'a, [u8]>>),
    L16(ImageBuffer<Luma<u16>, Cow<'a, [u16]>>),
    LA16(ImageBuffer<LumaA<u16>, Cow<'a, [u16]>>),
    RGB16(ImageBuffer<Rgb<u16>, Cow<'a, [u16]>>),
    RGBA16(ImageBuffer<Rgba<u16>, Cow<'a, [u16]>>),
    RGB32F(ImageBuffer<Rgb<f32>, Cow<'a, [f32]>>),
    RGBA32F(ImageBuffer<Rgba<f32>, Cow<'a, [f32]>>),
}

/// Dynamically downcast the underlying image buffer to a generic slice.
macro_rules! cow_as_generic_slice {
    (
    $img:expr, $T:ident
) => {{
        let img_vec = $img.as_raw() as &'a dyn std::any::Any;
        img_vec.downcast_ref::<&'a [$T]>().unwrap()
    }};
}

macro_rules! with_method_on_image_buffer {
    (
    $key_type:expr, $method: ident
) => {{
        use DaftImageBuffer::*;

        match $key_type {
            L(img) => img.$method(),
            LA(img) => img.$method(),
            RGB(img) => img.$method(),
            RGBA(img) => img.$method(),
            L16(img) => img.$method(),
            LA16(img) => img.$method(),
            RGB16(img) => img.$method(),
            RGBA16(img) => img.$method(),
            RGB32F(img) => img.$method(),
            RGBA32F(img) => img.$method(),
        }
    }};
}

impl<'a> DaftImageBuffer<'a> {
    pub fn height(&self) -> u32 {
        with_method_on_image_buffer!(self, height)
    }

    pub fn width(&self) -> u32 {
        with_method_on_image_buffer!(self, width)
    }

    pub fn channels(&self) -> u16 {
        use DaftImageBuffer::*;
        match self {
            L(..) | L16(..) => 1,
            LA(..) | LA16(..) => 2,
            RGB(..) | RGB16(..) | RGB32F(..) => 3,
            RGBA(..) | RGBA16(..) | RGBA32F(..) => 4,
        }
    }

    pub fn as_slice<T: arrow2::types::NativeType>(&'a self) -> &'a [T] {
        use DaftImageBuffer::*;
        match self {
            L(img) => cow_as_generic_slice!(img, T),
            LA(img) => cow_as_generic_slice!(img, T),
            RGB(img) => cow_as_generic_slice!(img, T),
            RGBA(img) => cow_as_generic_slice!(img, T),
            _ => unimplemented!("unimplemented {self:?}"),
        }
    }

    pub fn mode(&self) -> ImageMode {
        use DaftImageBuffer::*;
        match self {
            L(..) => ImageMode::L,
            LA(..) => ImageMode::LA,
            RGB(..) => ImageMode::RGB,
            RGBA(..) => ImageMode::RGBA,
            L16(..) => ImageMode::L16,
            LA16(..) => ImageMode::LA16,
            RGB16(..) => ImageMode::RGB16,
            RGBA16(..) => ImageMode::RGBA16,
            RGB32F(..) => ImageMode::RGB32F,
            RGBA32F(..) => ImageMode::RGBA32F,
        }
    }

    pub fn resize(&self, w: u32, h: u32) -> Self {
        use DaftImageBuffer::*;
        match self {
            L(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::L(image_buffer_vec_to_cow(result))
            }
            LA(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::LA(image_buffer_vec_to_cow(result))
            }
            RGB(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::RGB(image_buffer_vec_to_cow(result))
            }
            RGBA(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::RGBA(image_buffer_vec_to_cow(result))
            }
            _ => unimplemented!("mode not implemented"),
        }
    }
}

fn image_buffer_vec_to_cow<'a, P, T>(input: ImageBuffer<P, Vec<T>>) -> ImageBuffer<P, Cow<'a, [T]>>
where
    P: image::Pixel<Subpixel = T>,
    Vec<T>: Deref<Target = [P::Subpixel]>,
    T: ToOwned + std::clone::Clone,
    [T]: ToOwned,
{
    let h = input.height();
    let w = input.width();
    let owned: Cow<[T]> = input.into_raw().into();
    ImageBuffer::from_raw(w, h, owned).unwrap()
}

/// Dynamically downcast the underlying image buffer to a generic slice.
macro_rules! as_generic_slice {
    (
    $img:expr, $T:ident
) => {{
        let img_vec = $img.as_raw() as &dyn std::any::Any;
        img_vec.downcast_ref::<Vec<$T>>().unwrap().as_slice()
    }};
}

fn arr_to_cow_slice<'a, T: arrow2::types::NativeType>(
    da: &'a arrow2::array::ListArray<i64>,
    start: usize,
    end: usize,
) -> Cow<'a, [T]> {
    let values = da
        .values()
        .as_ref()
        .as_any()
        .downcast_ref::<arrow2::array::PrimitiveArray<T>>()
        .unwrap();
    Cow::Borrowed(&values.values().as_slice()[start..end] as &'a [T])
}

impl ImageArray {
    fn image_mode(&self) -> &Option<ImageMode> {
        match self.logical_type() {
            DataType::Image(_, mode) => mode,
            _ => panic!("Expected dtype to be Image"),
        }
    }

    fn data_array(&self) -> &arrow2::array::ListArray<i64> {
        let p = self.physical.as_arrow();
        const IMAGE_DATA_IDX: usize = 0;
        let array = p.values().get(IMAGE_DATA_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn channel_array(&self) -> &arrow2::array::UInt16Array {
        let p = self.physical.as_arrow();
        const IMAGE_CHANNEL_IDX: usize = 1;
        let array = p.values().get(IMAGE_CHANNEL_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn height_array(&self) -> &arrow2::array::UInt32Array {
        let p = self.physical.as_arrow();
        const IMAGE_HEIGHT_IDX: usize = 2;
        let array = p.values().get(IMAGE_HEIGHT_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn width_array(&self) -> &arrow2::array::UInt32Array {
        let p = self.physical.as_arrow();
        const IMAGE_WIDTH_IDX: usize = 3;
        let array = p.values().get(IMAGE_WIDTH_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn mode_array(&self) -> &arrow2::array::UInt8Array {
        let p = self.physical.as_arrow();
        const IMAGE_MODE_IDX: usize = 4;
        let array = p.values().get(IMAGE_MODE_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn as_image_obj<'a>(&'a self, idx: usize) -> Option<DaftImageBuffer<'a>> {
        assert!(idx < self.len());
        if !self.physical.is_valid(idx) {
            return None;
        }

        let da = self.data_array();
        let ca = self.channel_array();
        let ha = self.height_array();
        let wa = self.width_array();
        let ma = self.mode_array();

        let offsets = da.offsets();

        let start = *offsets.get(idx).unwrap() as usize;
        let end = *offsets.get(idx + 1).unwrap() as usize;

        let c = ca.value(idx);
        let h = ha.value(idx);
        let w = wa.value(idx);
        let m: ImageMode = ImageMode::from_u8(ma.value(idx)).unwrap();
        assert_eq!(m.num_channels(), c);
        let result = match m {
            ImageMode::L => {
                let slice_data = arr_to_cow_slice::<u8>(da, start, end);
                DaftImageBuffer::<'a>::L(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            ImageMode::LA => {
                let slice_data = arr_to_cow_slice::<u8>(da, start, end);
                DaftImageBuffer::<'a>::LA(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            ImageMode::RGB => {
                let slice_data = arr_to_cow_slice::<u8>(da, start, end);
                DaftImageBuffer::<'a>::RGB(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            ImageMode::RGBA => {
                let slice_data = arr_to_cow_slice::<u8>(da, start, end);
                DaftImageBuffer::<'a>::RGBA(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            _ => unimplemented!("{m} is currently not implemented!"),
        };

        assert_eq!(result.height(), h);
        assert_eq!(result.width(), w);
        assert_eq!(result.channels(), c);
        Some(result)
    }

    pub fn resize(&self, w: u32, h: u32) -> DaftResult<Self> {
        let value_dtype = match self.logical_type() {
            DataType::Image(dtype, _) => dtype,
            _ => unreachable!("ImageArray's logical type should always be DataType::Image()."),
        };
        let result = (0..self.len())
            .map(|i| self.as_image_obj(i))
            .map(|img| img.map(|img| img.resize(w, h)))
            .collect::<Vec<_>>();
        with_match_numeric_daft_types!(**value_dtype, |$T| {
            type Tgt = <$T as DaftNumericType>::Native;
            Self::from_daft_image_buffers::<Tgt>(
                self.name(),
                result.as_slice(),
                self.image_mode(),
                value_dtype,
            )
        })
    }

    pub fn from_dyn_images<T: arrow2::types::NativeType>(
        name: &str,
        dyn_images: Vec<Option<image::DynamicImage>>,
        num_rows: usize,
        dtype: &DataType,
    ) -> DaftResult<Self> {
        let mut img_values = Vec::<&[T]>::with_capacity(num_rows);
        let mut offsets = Vec::<i64>::with_capacity(num_rows + 1);
        offsets.push(0i64);
        let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(num_rows);
        let mut channels = Vec::<u16>::with_capacity(num_rows);
        let mut heights = Vec::<u32>::with_capacity(num_rows);
        let mut widths = Vec::<u32>::with_capacity(num_rows);
        let mut modes = Vec::<u8>::with_capacity(num_rows);
        for dyn_img in dyn_images.iter() {
            validity.push(dyn_img.is_some());
            let (height, width, mode, values) = match dyn_img {
                Some(dyn_img) => {
                    let values = match dyn_img {
                        image::DynamicImage::ImageLuma8(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageLumaA8(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageRgb8(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageRgba8(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageLuma16(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageLumaA16(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageRgb16(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageRgba16(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageRgb32F(img) => as_generic_slice!(img, T),
                        image::DynamicImage::ImageRgba32F(img) => as_generic_slice!(img, T),
                        _ => todo!("not implemented"),
                    };
                    (
                        dyn_img.height(),
                        dyn_img.width(),
                        ImageMode::try_from(&dyn_img.color())?,
                        values,
                    )
                }
                None => (0u32, 0u32, ImageMode::L, &[] as &[T]),
            };
            heights.push(height);
            widths.push(width);
            modes.push(mode as u8);
            channels.push(mode.num_channels());
            img_values.push(values);
            offsets.push(offsets.last().unwrap() + values.len() as i64);
        }
        let validity: Option<arrow2::bitmap::Bitmap> = match validity.unset_bits() {
            0 => None,
            _ => Some(validity.into()),
        };
        let arrow_dtype = dtype.to_arrow()?;
        let values_array = arrow2::array::PrimitiveArray::from_vec(img_values.concat()).boxed();

        let data_dtype = arrow2::datatypes::DataType::LargeList(Box::new(
            arrow2::datatypes::Field::new("data", arrow_dtype.clone(), true),
        ));

        let data_array = Box::new(arrow2::array::ListArray::new(
            data_dtype.clone(),
            arrow2::offset::OffsetsBuffer::try_from(offsets)?,
            values_array,
            validity.clone(),
        ));
        let channel_array = Box::new(arrow2::array::PrimitiveArray::from_vec(channels));
        let height_array = Box::new(arrow2::array::PrimitiveArray::from_vec(heights));
        let width_array = Box::new(arrow2::array::PrimitiveArray::from_vec(widths));
        let mode_array = Box::new(arrow2::array::PrimitiveArray::from_vec(modes));
        let struct_dtype = arrow2::datatypes::DataType::Struct(vec![
            arrow2::datatypes::Field::new("data", data_dtype, true),
            arrow2::datatypes::Field::new("channel", channel_array.data_type().clone(), true),
            arrow2::datatypes::Field::new("height", height_array.data_type().clone(), true),
            arrow2::datatypes::Field::new("width", width_array.data_type().clone(), true),
            arrow2::datatypes::Field::new("mode", mode_array.data_type().clone(), true),
        ]);

        let daft_type = (&struct_dtype).into();

        let struct_array = arrow2::array::StructArray::new(
            struct_dtype,
            vec![
                data_array,
                channel_array,
                height_array,
                width_array,
                mode_array,
            ],
            validity,
        );

        let daft_struct_array =
            StructArray::new(Field::new("item", daft_type).into(), Box::new(struct_array))?;
        Ok(Self::new(
            Field::new(name, DataType::Image(Box::new(dtype.clone()), None)),
            daft_struct_array,
        ))
    }

    fn from_daft_image_buffers<T: arrow2::types::NativeType>(
        name: &str,
        inputs: &[Option<DaftImageBuffer<'_>>],
        image_mode: &Option<ImageMode>,
        value_dtype: &Box<DataType>,
    ) -> DaftResult<Self> {
        use DaftImageBuffer::*;
        // let is_all_u8 = inputs
        //     .iter()
        //     .filter_map(|b| b.as_ref())
        //     .all(|b| matches!(b, L(..) | LA(..) | RGB(..) | RGBA(..)));
        // assert!(is_all_u8);

        let mut data_ref = Vec::with_capacity(inputs.len());
        let mut heights = Vec::with_capacity(inputs.len());
        let mut channels = Vec::with_capacity(inputs.len());
        let mut modes = Vec::with_capacity(inputs.len());
        let mut widths = Vec::with_capacity(inputs.len());
        let mut offsets = Vec::with_capacity(inputs.len() + 1);
        let mut is_valid = Vec::with_capacity(inputs.len());
        offsets.push(0i64);

        for ib in inputs {
            if let Some(ib) = ib {
                heights.push(ib.height());
                widths.push(ib.width());
                channels.push(ib.channels());

                let buffer = ib.as_slice::<T>();
                data_ref.push(buffer);
                offsets.push(buffer.len() as i64 + offsets.last().unwrap());
                modes.push(ib.mode() as u8);
                is_valid.push(true);
            } else {
                heights.push(0u32);
                widths.push(0u32);
                channels.push(0u16);
                offsets.push(*offsets.last().unwrap());
                modes.push(0);
                is_valid.push(false);
            }
        }

        let collected_data = data_ref.concat();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let data_type = crate::datatypes::DataType::Image(
            Box::new(crate::datatypes::DataType::UInt8),
            *image_mode,
        );

        let validity = arrow2::bitmap::Bitmap::from(is_valid);
        let arrow_dtype = value_dtype.to_arrow()?;

        let list_datatype = arrow2::datatypes::DataType::LargeList(Box::new(
            arrow2::datatypes::Field::new("data", arrow_dtype.clone(), true),
        ));
        let data_array = Box::new(arrow2::array::ListArray::<i64>::new(
            list_datatype,
            offsets,
            Box::new(arrow2::array::PrimitiveArray::from_vec(collected_data)),
            Some(validity.clone()),
        ));

        let values: Vec<Box<dyn arrow2::array::Array>> = vec![
            data_array,
            Box::new(
                arrow2::array::UInt16Array::from_vec(channels)
                    .with_validity(Some(validity.clone())),
            ),
            Box::new(
                arrow2::array::UInt32Array::from_vec(heights).with_validity(Some(validity.clone())),
            ),
            Box::new(
                arrow2::array::UInt32Array::from_vec(widths).with_validity(Some(validity.clone())),
            ),
            Box::new(
                arrow2::array::UInt8Array::from_vec(modes).with_validity(Some(validity.clone())),
            ),
        ];
        let physical_type = data_type.to_physical();
        let struct_array = Box::new(arrow2::array::StructArray::new(
            physical_type.to_arrow()?,
            values,
            Some(validity),
        ));

        let daft_struct_array = crate::datatypes::StructArray::new(
            Field::new(name, physical_type).into(),
            struct_array,
        )?;
        Ok(ImageArray::new(
            Field::new(name, data_type),
            daft_struct_array,
        ))
    }
}
