//! Strict flat-object JSON: string, unsigned integer, true/false/null values.
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Val {
    Str(String),
    Num(u64),
    Bool(bool),
    Null,
}

fn corrupt(m: &str) -> Error {
    Error::Corrupt(format!("json: {m}"))
}

pub(crate) fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
    out
}

struct P<'a> {
    s: &'a [u8],
    i: usize,
}

impl P<'_> {
    fn ws(&mut self) {
        while matches!(self.s.get(self.i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }
    fn eat(&mut self, c: u8) -> Result<()> {
        self.ws();
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            Ok(())
        } else {
            Err(corrupt(&format!("expected '{}'", c as char)))
        }
    }
    fn string(&mut self) -> Result<String> {
        self.eat(b'"')?;
        let mut out = Vec::new();
        loop {
            let c = *self
                .s
                .get(self.i)
                .ok_or_else(|| corrupt("unterminated string"))?;
            self.i += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let e = *self.s.get(self.i).ok_or_else(|| corrupt("bad escape"))?;
                    self.i += 1;
                    match e {
                        b'"' | b'\\' => out.push(e),
                        _ => return Err(corrupt("unsupported escape")),
                    }
                }
                0..=0x1f => return Err(corrupt("control character in string")),
                _ => out.push(c),
            }
        }
        String::from_utf8(out).map_err(|_| corrupt("invalid utf-8"))
    }
    fn literal(&mut self, word: &str) -> Result<()> {
        if self.s[self.i..].starts_with(word.as_bytes()) {
            self.i += word.len();
            Ok(())
        } else {
            Err(corrupt("bad value"))
        }
    }
    fn value(&mut self) -> Result<Val> {
        self.ws();
        match self.s.get(self.i) {
            Some(b'"') => Ok(Val::Str(self.string()?)),
            Some(b't') => self.literal("true").map(|_| Val::Bool(true)),
            Some(b'f') => self.literal("false").map(|_| Val::Bool(false)),
            Some(b'n') => self.literal("null").map(|_| Val::Null),
            Some(b'0'..=b'9') => {
                let start = self.i;
                while matches!(self.s.get(self.i), Some(b'0'..=b'9')) {
                    self.i += 1;
                }
                let digits = &self.s[start..self.i];
                if digits.len() > 1 && digits[0] == b'0' {
                    return Err(corrupt("leading zero"));
                }
                // Digits are ASCII, so the str conversion cannot fail.
                std::str::from_utf8(digits)
                    .unwrap()
                    .parse()
                    .map(Val::Num)
                    .map_err(|_| corrupt("number out of range"))
            }
            _ => Err(corrupt("unsupported value")),
        }
    }
}

/// Parse a flat object. Key order is free; a duplicate key is Corrupt.
pub(crate) fn parse_object(b: &[u8]) -> Result<Vec<(String, Val)>> {
    let mut p = P { s: b, i: 0 };
    p.eat(b'{')?;
    let mut out: Vec<(String, Val)> = Vec::new();
    p.ws();
    if p.s.get(p.i) == Some(&b'}') {
        p.i += 1;
    } else {
        loop {
            let k = p.string()?;
            p.eat(b':')?;
            let v = p.value()?;
            if out.iter().any(|(ek, _)| *ek == k) {
                return Err(corrupt(&format!("duplicate key {k}")));
            }
            out.push((k, v));
            p.ws();
            match p.s.get(p.i) {
                Some(b',') => p.i += 1,
                Some(b'}') => {
                    p.i += 1;
                    break;
                }
                _ => return Err(corrupt("expected ',' or '}'")),
            }
        }
    }
    p.ws();
    if p.i != p.s.len() {
        return Err(corrupt("trailing data"));
    }
    Ok(out)
}

/// Field lookup helpers over a parsed object.
pub(crate) struct Obj(pub Vec<(String, Val)>);

impl Obj {
    pub(crate) fn parse(b: &[u8]) -> Result<Obj> {
        parse_object(b).map(Obj)
    }
    fn get(&self, key: &str) -> Result<&Val> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
            .ok_or_else(|| corrupt(&format!("missing key {key}")))
    }
    pub(crate) fn num(&self, key: &str) -> Result<u64> {
        match self.get(key)? {
            Val::Num(n) => Ok(*n),
            _ => Err(corrupt(&format!("{key} is not a number"))),
        }
    }
    pub(crate) fn str(&self, key: &str) -> Result<&str> {
        match self.get(key)? {
            Val::Str(s) => Ok(s),
            _ => Err(corrupt(&format!("{key} is not a string"))),
        }
    }
    pub(crate) fn bool(&self, key: &str) -> Result<bool> {
        match self.get(key)? {
            Val::Bool(b) => Ok(*b),
            _ => Err(corrupt(&format!("{key} is not a bool"))),
        }
    }
    /// A string, or None for null.
    pub(crate) fn opt_str(&self, key: &str) -> Result<Option<&str>> {
        match self.get(key)? {
            Val::Str(s) => Ok(Some(s)),
            Val::Null => Ok(None),
            _ => Err(corrupt(&format!("{key} is not a string or null"))),
        }
    }
}
