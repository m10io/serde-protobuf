//! Types for representing runtime Protobuf values.
use std::collections;
use std::convert::TryInto;

use protobuf;
use protobuf::stream::wire_format;

use crate::descriptor;
use crate::error;

/// Any protobuf value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// A boolean value.
    Bool(bool),
    /// A 32-bit signed integer.
    I32(i32),
    /// A 64-bit signed integer.
    I64(i64),
    /// A 32-bit unsigned integer.
    U32(u32),
    /// A 64-bit unsigned integer.
    U64(u64),
    /// A 32-bit floating point value.
    F32(f32),
    /// A 64-bit floating point value.
    F64(f64),
    /// A 64-bit signed integer that is more efficent for signed values.
    S64(i64),
    /// A 32-bit signed integer that is more efficent for signed values.
    S32(i32),
    /// A 64-bit fixed length unsigned integer.
    Fixed64(u64),
    /// A 32-bit fixed length unsigned integer.
    Fixed32(u32),
    /// A 64-bit fixed length signed integer.
    SFixed64(i64),
    /// A 32-bit fixed length signed integer.
    SFixed32(i32),
    /// A byte vector.
    Bytes(Vec<u8>),
    /// A string.
    String(String),
    /// An enum value.
    Enum(i32),
    /// A message.
    Message(Message),
}

/// A message value.
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    /// Known fields on the message.
    pub fields: collections::BTreeMap<i32, Field>,
    /// Unknown fields on the message.
    pub unknown: protobuf::UnknownFields,
    /// The message descriptor for this message
    pub descriptor: descriptor::MessageDescriptor,
}

/// A message field value.
#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    /// A field with a single value.
    Singular(Option<Value>),
    /// A field with several (repeated) values.
    Repeated(Vec<Value>),
}

impl Message {
    /// Creates a message given a Protobuf descriptor.
    #[inline]
    pub fn new(message: descriptor::MessageDescriptor) -> Message {
        let mut m = Message {
            fields: collections::BTreeMap::new(),
            unknown: protobuf::UnknownFields::new(),
            descriptor: message,
        };

        for field in m.descriptor.fields() {
            m.fields.insert(
                field.number(),
                if field.is_repeated() {
                    Field::Repeated(Vec::new())
                } else {
                    Field::Singular(field.default_value().cloned())
                },
            );
        }

        m
    }

    /// Merge data from the given input stream into this message.
    #[inline]
    pub fn merge_from(
        &mut self,
        descriptors: &descriptor::Descriptors,
        message: &descriptor::MessageDescriptor,
        input: &mut protobuf::CodedInputStream,
    ) -> error::Result<()> {
        while !input.eof()? {
            let (number, wire_type) = input.read_tag_unpack()?;

            if let Some(field) = message.field_by_number(number as i32) {
                let value = self.ensure_field(field);
                value.merge_from(descriptors, field, input, wire_type)?;
            } else {
                use protobuf::rt::read_unknown_or_skip_group as u;
                u(number, wire_type, input, &mut self.unknown)?;
            }
        }
        Ok(())
    }

    /// Merge data from given input stream with a FieldMask
    #[inline]
    pub fn merge_from_masked(
        &mut self,
        descriptors: &descriptor::Descriptors,
        message: &descriptor::MessageDescriptor,
        input: &mut protobuf::CodedInputStream,
        mask: Vec<&[&str]>,
    ) -> error::Result<()> {
        let mut trash_fields = protobuf::UnknownFields::new();
        while !input.eof()? {
            let (number, wire_type) = input.read_tag_unpack()?;
            if let Some(field_desc) = message.field_by_number(number.try_into().map_err(|_| {
                crate::error::Error::Custom {
                    message: "field was negative".to_string(),
                }
            })?) {
                if mask.iter().any(|p| p.first() == Some(&field_desc.name())) {
                    if let Some(field) = message.field_by_number(number as i32) {
                        let value = self.ensure_field(field);
                        let new_mask = mask
                            .iter()
                            .filter_map(|p| {
                                if p.len() > 1 && p.first() == Some(&field_desc.name()) {
                                    Some(&p[1..])
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>();

                        if new_mask.is_empty() {
                            value.merge_from(descriptors, field, input, wire_type)?;
                        } else {
                            value.merge_from_masked(
                                descriptors,
                                field,
                                input,
                                wire_type,
                                Some(new_mask),
                            )?;
                        }
                    } else {
                        protobuf::rt::read_unknown_or_skip_group(
                            number,
                            wire_type,
                            input,
                            &mut trash_fields,
                        )?;
                    }
                } else {
                    protobuf::rt::read_unknown_or_skip_group(
                        number,
                        wire_type,
                        input,
                        &mut trash_fields,
                    )?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn ensure_field(&mut self, field: &descriptor::FieldDescriptor) -> &mut Field {
        self.fields
            .entry(field.number())
            .or_insert_with(|| Field::new(field))
    }
}

impl Field {
    /// Creates a field given a Protobuf descriptor.
    #[inline]
    pub fn new(field: &descriptor::FieldDescriptor) -> Field {
        if field.is_repeated() {
            Field::Repeated(Vec::new())
        } else {
            Field::Singular(None)
        }
    }

    #[inline]

    /// Merge data from the given input stream into this field with an optional mask.
    #[inline]
    pub fn merge_from_masked(
        &mut self,
        descriptors: &descriptor::Descriptors,
        field: &descriptor::FieldDescriptor,
        input: &mut protobuf::CodedInputStream,
        wire_type: protobuf::stream::wire_format::WireType,
        mask: Option<Vec<&[&str]>>,
    ) -> error::Result<()> {
        // Make the type dispatch below more compact
        use crate::descriptor::FieldType::*;
        use protobuf::stream::wire_format::WireType::*;
        use protobuf::CodedInputStream as I;

        // Singular scalar
        macro_rules! ss {
            ($expected_wire_type:expr, $visit_func:expr, $reader:expr) => {
                self.merge_scalar(input, wire_type, $expected_wire_type, $visit_func, $reader)
            };
        }

        // Packable scalar
        macro_rules! ps {
            ($expected_wire_type:expr, $visit_func:expr, $reader:expr) => {
                self.merge_packable_scalar(
                    input,
                    wire_type,
                    $expected_wire_type,
                    $visit_func,
                    $reader,
                )
            };
            ($expected_wire_type:expr, $size:expr, $visit_func:expr, $reader:expr) => {
                // TODO: use size to pre-allocate buffer space
                self.merge_packable_scalar(
                    input,
                    wire_type,
                    $expected_wire_type,
                    $visit_func,
                    $reader,
                )
            };
        }

        match field.field_type(descriptors) {
            Bool => ps!(WireTypeVarint, Value::Bool, I::read_bool),
            Int32 => ps!(WireTypeVarint, Value::I32, I::read_int32),
            Int64 => ps!(WireTypeVarint, Value::I64, I::read_int64),
            SInt32 => ps!(WireTypeVarint, Value::S32, I::read_sint32),
            SInt64 => ps!(WireTypeVarint, Value::S64, I::read_sint64),
            UInt32 => ps!(WireTypeVarint, Value::U32, I::read_uint32),
            UInt64 => ps!(WireTypeVarint, Value::U64, I::read_uint64),
            Fixed32 => ps!(WireTypeFixed32, 4, Value::Fixed32, I::read_fixed32),
            Fixed64 => ps!(WireTypeFixed64, 8, Value::Fixed64, I::read_fixed64),
            SFixed32 => ps!(WireTypeFixed32, 4, Value::SFixed32, I::read_sfixed32),
            SFixed64 => ps!(WireTypeFixed64, 8, Value::SFixed64, I::read_sfixed64),
            Float => ps!(WireTypeFixed32, 4, Value::F32, I::read_float),
            Double => ps!(WireTypeFixed64, 8, Value::F64, I::read_double),
            Bytes => ss!(WireTypeLengthDelimited, Value::Bytes, I::read_bytes),
            String => ss!(WireTypeLengthDelimited, Value::String, I::read_string),
            Enum(_) => self.merge_enum(input, wire_type),
            Message(ref m) => {
                if let Some(mask) = mask {
                    self.merge_message_masked(input, descriptors, m, wire_type, mask)
                } else {
                    self.merge_message(input, descriptors, m, wire_type)
                }
            }
            Group => unimplemented!(),
            UnresolvedEnum(e) => Err(error::Error::UnknownEnum { name: e.to_owned() }),
            UnresolvedMessage(m) => Err(error::Error::UnknownMessage { name: m.to_owned() }),
        }
    }

    /// Merge data from the given input stream into this field.
    #[inline]
    pub fn merge_from(
        &mut self,
        descriptors: &descriptor::Descriptors,
        field: &descriptor::FieldDescriptor,
        input: &mut protobuf::CodedInputStream,
        wire_type: protobuf::stream::wire_format::WireType,
    ) -> error::Result<()> {
        self.merge_from_masked(descriptors, field, input, wire_type, None)
    }

    #[inline]
    fn merge_scalar<'a, A, V, R>(
        &mut self,
        input: &mut protobuf::CodedInputStream<'a>,
        actual_wire_type: wire_format::WireType,
        expected_wire_type: wire_format::WireType,
        value_ctor: V,
        reader: R,
    ) -> error::Result<()>
    where
        V: Fn(A) -> Value,
        R: Fn(&mut protobuf::CodedInputStream<'a>) -> protobuf::ProtobufResult<A>,
    {
        if expected_wire_type == actual_wire_type {
            self.put(value_ctor(reader(input)?));
            Ok(())
        } else {
            Err(error::Error::BadWireType {
                wire_type: actual_wire_type,
            })
        }
    }

    #[inline]
    fn merge_packable_scalar<'a, A, V, R>(
        &mut self,
        input: &mut protobuf::CodedInputStream<'a>,
        actual_wire_type: wire_format::WireType,
        expected_wire_type: wire_format::WireType,
        value_ctor: V,
        reader: R,
    ) -> error::Result<()>
    where
        V: Fn(A) -> Value,
        R: Fn(&mut protobuf::CodedInputStream<'a>) -> protobuf::ProtobufResult<A>,
    {
        if wire_format::WireType::WireTypeLengthDelimited == actual_wire_type {
            let len = input.read_raw_varint64()?;

            let old_limit = input.push_limit(len)?;
            while !input.eof()? {
                self.put(value_ctor(reader(input)?));
            }
            input.pop_limit(old_limit);

            Ok(())
        } else {
            self.merge_scalar(
                input,
                actual_wire_type,
                expected_wire_type,
                value_ctor,
                reader,
            )
        }
    }

    #[inline]
    fn merge_enum(
        &mut self,
        input: &mut protobuf::CodedInputStream,
        actual_wire_type: wire_format::WireType,
    ) -> error::Result<()> {
        if wire_format::WireType::WireTypeVarint == actual_wire_type {
            let v = input.read_raw_varint32()? as i32;
            self.put(Value::Enum(v));
            Ok(())
        } else {
            Err(error::Error::BadWireType {
                wire_type: actual_wire_type,
            })
        }
    }

    #[inline]
    fn merge_message(
        &mut self,
        input: &mut protobuf::CodedInputStream,
        descriptors: &descriptor::Descriptors,
        message: &descriptor::MessageDescriptor,
        actual_wire_type: wire_format::WireType,
    ) -> error::Result<()> {
        if wire_format::WireType::WireTypeLengthDelimited == actual_wire_type {
            let len = input.read_raw_varint64()?;
            let mut msg = match *self {
                Field::Singular(ref mut o) => {
                    if let Some(Value::Message(m)) = o.take() {
                        m
                    } else {
                        Message::new(message.clone())
                    }
                }
                _ => Message::new(message.clone()),
            };

            let old_limit = input.push_limit(len)?;
            msg.merge_from(descriptors, message, input)?;
            input.pop_limit(old_limit);

            self.put(Value::Message(msg));
            Ok(())
        } else {
            Err(error::Error::BadWireType {
                wire_type: actual_wire_type,
            })
        }
    }

    #[inline]
    fn merge_message_masked(
        &mut self,
        input: &mut protobuf::CodedInputStream,
        descriptors: &descriptor::Descriptors,
        message: &descriptor::MessageDescriptor,
        actual_wire_type: wire_format::WireType,
        mask: Vec<&[&str]>,
    ) -> error::Result<()> {
        if wire_format::WireType::WireTypeLengthDelimited == actual_wire_type {
            let len = input.read_raw_varint64()?;
            let mut msg = match *self {
                Field::Singular(ref mut o) => {
                    if let Some(Value::Message(m)) = o.take() {
                        m
                    } else {
                        Message::new(message.clone())
                    }
                }
                _ => Message::new(message.clone()),
            };

            let old_limit = input.push_limit(len)?;
            msg.merge_from_masked(descriptors, message, input, mask)?;
            input.pop_limit(old_limit);

            self.put(Value::Message(msg));
            Ok(())
        } else {
            Err(error::Error::BadWireType {
                wire_type: actual_wire_type,
            })
        }
    }

    #[inline]
    fn put(&mut self, value: Value) {
        match *self {
            Field::Singular(ref mut s) => *s = Some(value),
            Field::Repeated(ref mut r) => r.push(value),
        }
    }
}

impl Message {
    /// Writes the message to a [`protobuf::CodedOutputStream`] without a tag
    pub fn write_to_raw(
        self,
        stream: &mut protobuf::CodedOutputStream,
    ) -> protobuf::error::ProtobufResult<()> {
        for (tag, field) in self.fields.into_iter() {
            let tag: u32 = tag.try_into().map_err(|_| {
                protobuf::error::ProtobufError::WireError(protobuf::error::WireError::IncorrectTag(
                    0,
                ))
            })?;
            field.write_to(tag, stream)?;
        }
        Ok(())
    }

    /// Serializes the message into a vetor of bytes
    pub fn into_vec(self) -> protobuf::error::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        let mut stream = protobuf::CodedOutputStream::vec(&mut buf);
        self.write_to_raw(&mut stream)?;
        stream.flush()?;
        Ok(buf)
    }
}

impl Field {
    /// Writes a value to a stream
    pub fn write_to(
        self,
        tag: u32,
        stream: &mut protobuf::CodedOutputStream,
    ) -> protobuf::error::ProtobufResult<()> {
        match self {
            Field::Singular(Some(value)) => {
                value.write_to(tag, stream)?;
            }
            Field::Singular(None) => {}
            Field::Repeated(values) => match values.first() {
                Some(Value::Bool(_))
                | Some(Value::I32(_))
                | Some(Value::I64(_))
                | Some(Value::S32(_))
                | Some(Value::S64(_))
                | Some(Value::U32(_))
                | Some(Value::U64(_))
                | Some(Value::F32(_))
                | Some(Value::F64(_))
                | Some(Value::SFixed32(_))
                | Some(Value::SFixed64(_))
                | Some(Value::Fixed32(_))
                | Some(Value::Fixed64(_)) => {
                    values[0].write_tag(tag, stream)?;
                    for value in values.into_iter() {
                        value.write_to_raw(stream)?;
                    }
                }
                _ => {
                    for value in values.into_iter() {
                        value.write_to(tag, stream)?;
                    }
                }
            },
        }
        Ok(())
    }

    /// Serializes the message into a vetor of bytes
    pub fn into_vec(self) -> protobuf::error::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        let mut stream = protobuf::CodedOutputStream::vec(&mut buf);
        self.write_to(1, &mut stream)?;
        stream.flush()?;
        Ok(buf)
    }
}

impl Value {
    fn write_tag(
        &self,
        tag: u32,
        stream: &mut protobuf::CodedOutputStream,
    ) -> protobuf::error::ProtobufResult<()> {
        match self {
            Value::Bool(_)
            | Value::I32(_)
            | Value::I64(_)
            | Value::U32(_)
            | Value::U64(_)
            | Value::Enum(_) => stream.write_tag(tag, wire_format::WireType::WireTypeVarint)?,
            Value::F64(_) | Value::S64(_) | Value::Fixed64(_) | Value::SFixed64(_) => {
                stream.write_tag(tag, wire_format::WireType::WireTypeFixed64)?
            }
            Value::F32(_) | Value::S32(_) | Value::Fixed32(_) | Value::SFixed32(_) => {
                stream.write_tag(tag, wire_format::WireType::WireTypeFixed32)?
            }
            Value::String(_) | Value::Message(_) | Value::Bytes(_) => {
                stream.write_tag(tag, wire_format::WireTypeLengthDelimited)?
            }
        };
        Ok(())
    }

    /// Writes a value to a stream
    pub fn write_to(
        self,
        tag: u32,
        stream: &mut protobuf::CodedOutputStream,
    ) -> protobuf::error::ProtobufResult<()> {
        self.write_tag(tag, stream)?;
        self.write_to_raw(stream)
    }

    /// Writes a value to a stream without the tag
    pub fn write_to_raw(
        self,
        stream: &mut protobuf::CodedOutputStream,
    ) -> protobuf::error::ProtobufResult<()> {
        match self {
            Value::Bool(b) => stream.write_bool_no_tag(b)?,
            Value::I32(i) => stream.write_int32_no_tag(i)?,
            Value::I64(i) => stream.write_int64_no_tag(i)?,
            Value::U64(i) => stream.write_uint64_no_tag(i)?,
            Value::U32(i) => stream.write_uint32_no_tag(i)?,
            Value::S64(i) => stream.write_sint64_no_tag(i)?,
            Value::S32(i) => stream.write_sint32_no_tag(i)?,
            Value::Fixed64(i) => stream.write_fixed64_no_tag(i)?,
            Value::Fixed32(i) => stream.write_fixed32_no_tag(i)?,
            Value::SFixed64(i) => stream.write_sfixed64_no_tag(i)?,
            Value::SFixed32(i) => stream.write_sfixed32_no_tag(i)?,
            Value::String(s) => stream.write_string_no_tag(&s)?,
            Value::F32(f) => stream.write_float_no_tag(f)?,
            Value::F64(f) => stream.write_double_no_tag(f)?,
            Value::Bytes(b) => stream.write_raw_bytes(&b)?,
            Value::Enum(e) => stream.write_enum_no_tag(e)?,
            Value::Message(m) => m.write_to_raw(stream)?,
        };
        Ok(())
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct(self.descriptor.name());
        self.descriptor.fields()
            .iter()
            .fold(&mut f, |f, field_desc| {
                if let Some(field) = self.fields.get(&field_desc.number()) {
                    f.field(field_desc.name(), &field)
                }else{
                    f
                }
            })
            .finish()
    }
}

