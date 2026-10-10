//! The one binary form of values inside the engine: keys of `group`,
//! `distinct` and `pivot` (whose bytes order the groups), partial group
//! states and spilled rows. Lengths and fixed-size numbers are big-endian
//! `u64`; counts in compact states are varints. The storage keeps its own
//! format.
#![cfg_attr(rustfmt, rustfmt_skip)]
use super::{ExecutionError, ExecutionResult};
use crate::model::{Document, Number, Value};

pub(crate) fn encode_len(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(&(value as u64).to_be_bytes());
}

pub(crate) fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    encode_len(output, bytes.len());
    output.extend_from_slice(bytes);
}

pub(crate) fn encode_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

pub(crate) fn encode_document(output: &mut Vec<u8>, document: &Document) {
    encode_len(output, document.len());
    for (name, value) in document {
        encode_bytes(output, name.as_str().as_bytes());
        encode_value(output, value);
    }
}

pub(crate) fn encode_value(output: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => output.push(0),
        Value::Bool(flag) => output.push(1 + u8::from(*flag)),
        Value::Number(Number::Signed(value)) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Value::Number(Number::Unsigned(value)) => {
            output.push(4);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Value::Number(Number::Float(value)) => {
            output.push(5);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::String(value) => {
            output.push(6);
            encode_bytes(output, value.as_bytes());
        }
        Value::Array(values) => {
            output.push(7);
            encode_len(output, values.len());
            for value in values.iter() {
                encode_value(output, value);
            }
        }
        Value::Object(document) => {
            output.push(8);
            encode_document(output, document);
        }
    }
}

/// [`encode_value`] of a value borrowed from a projected column.
pub(crate) fn encode_projected_ref(output: &mut Vec<u8>, value: &crate::storage::ProjectedValueRef<'_>) {
    use crate::storage::ProjectedValueRef;
    match value {
        ProjectedValueRef::Null => output.push(0),
        ProjectedValueRef::Bool(false) => output.push(1),
        ProjectedValueRef::Bool(true) => output.push(2),
        ProjectedValueRef::Signed(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        ProjectedValueRef::Unsigned(value) => {
            output.push(4);
            output.extend_from_slice(&value.to_be_bytes());
        }
        ProjectedValueRef::Float(value) => {
            output.push(5);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        ProjectedValueRef::String(value) => {
            output.push(6);
            encode_bytes(output, value.as_bytes());
        }
        ProjectedValueRef::Owned(value) => encode_value(output, value),
    }
}

/// Reads what the `encode_*` functions wrote.
pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

fn invalid(what: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::evaluation(format!("invalid encoded value: {what}"))
}

impl<'a> Decoder<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    pub(crate) fn take(&mut self, count: usize) -> ExecutionResult<&'a [u8]> {
        let bytes = self.position.checked_add(count).and_then(|end| self.bytes.get(self.position..end)).ok_or_else(|| invalid("truncated"))?;
        self.position += count;
        Ok(bytes)
    }

    /// The bytes not read yet.
    pub(crate) fn rest(self) -> &'a [u8] {
        &self.bytes[self.position..]
    }

    pub(crate) fn array<const N: usize>(&mut self) -> ExecutionResult<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("N bytes"))
    }

    pub(crate) fn byte(&mut self) -> ExecutionResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u64(&mut self) -> ExecutionResult<u64> {
        self.array().map(u64::from_be_bytes)
    }

    pub(crate) fn len(&mut self) -> ExecutionResult<usize> {
        usize::try_from(self.u64()?).map_err(|_| invalid("length exceeds usize"))
    }

    pub(crate) fn varint(&mut self) -> ExecutionResult<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(invalid("varint overflow"))
    }

    /// A varint size.
    pub(crate) fn size(&mut self) -> ExecutionResult<usize> {
        usize::try_from(self.varint()?).map_err(|_| invalid("size exceeds usize"))
    }

    pub(crate) fn str(&mut self) -> ExecutionResult<&'a str> {
        let len = self.len()?;
        std::str::from_utf8(self.take(len)?).map_err(invalid)
    }

    pub(crate) fn document(&mut self) -> ExecutionResult<Document> {
        let mut document = Document::new();
        for _ in 0..self.len()? {
            let name = self.str()?;
            document.insert(name, self.value()?);
        }
        Ok(document)
    }

    pub(crate) fn value(&mut self) -> ExecutionResult<Value> {
        Ok(match self.byte()? {
            0 => Value::null(),
            tag @ (1 | 2) => Value::bool(tag == 2),
            3 => Value::signed(i64::from_be_bytes(self.array()?)),
            4 => Value::unsigned(self.u64()?),
            5 => Value::float(f64::from_bits(self.u64()?)).map_err(invalid)?,
            6 => Value::string(self.str()?),
            7 => Value::array((0..self.len()?).map(|_| self.value()).collect::<ExecutionResult<Vec<_>>>()?),
            8 => Value::object(self.document()?),
            tag => return Err(invalid(format!("tag {tag}"))),
        })
    }

    pub(crate) fn finish(self) -> ExecutionResult<()> {
        if self.is_empty() { Ok(()) } else { Err(invalid("trailing bytes")) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn values_read_back_as_written() { let mut nested = Document::new(); nested.insert("k", Value::array([Value::from(-3_i64), Value::from(7_u64), Value::float(2.5).unwrap(), Value::null(), Value::from(true), Value::from("é")])); let mut output = Vec::new(); encode_value(&mut output, &Value::object(nested.clone())); encode_varint(&mut output, 300); let mut input = Decoder::new(&output); assert_eq!(input.value().unwrap(), Value::object(nested)); assert_eq!(input.varint().unwrap(), 300); input.finish().unwrap(); assert!(Decoder::new(&output[..5]).value().is_err()); }
}
