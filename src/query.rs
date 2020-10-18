use bson::{doc, Bson, Document};
use std::ops;

/// Checks if the specified field is present in the struct.
/// Borrowed from `mongodm`.
#[macro_export]
macro_rules! field {
  ($field:ident in $type:path) => {{
    const _: fn() = || {
      let $type { $field: _, .. };
    };
    stringify!($field)
  }};
}

/// Shorthand for `field!`.
/// Borrowed from `mongodm`.
#[macro_export]
macro_rules! f {
  ($field:ident in $type:path) => {
    crate::field!($field in $type);
  };
}

/// Convenience methods to construct Queries from `&str` and `String`.
///
/// Example:
/// ```
/// fn index() {
///   let filter = "author".is_in(["Author A", "Author B", ...]);
///   filter.then("uploaded_at".more_than(""))
/// }
/// ```
pub trait ToQuery
where
  Self: Into<String>,
{
  /// Equivalence. `{a: b}`
  fn is<I: Into<Bson>>(self, other: I) -> Query {
    Query::Is(self.into(), other.into())
  }
  /// Non-equivalence. `{a: {$ne: b}}`
  fn not<T: Into<Bson>>(self, other: T) -> Query {
    Query::Ne(self.into(), other.into())
  }
  /// Greater than. `{a: {$gt: b}}`
  fn more_than<T: Into<Bson>>(self, other: T) -> Query {
    Query::Gt(self.into(), other.into())
  }
  /// Greater than or equal. `{a: {$gte: b}}`
  fn gte<T: Into<Bson>>(self, other: T) -> Query {
    Query::Gte(self.into(), other.into())
  }
  /// Less than. `{a: {$lt: b}}`
  fn less_than<T: Into<Bson>>(self, other: T) -> Query {
    Query::Lt(self.into(), other.into())
  }
  /// Less than or equal. `{a: {$lte: b}}`
  fn lte<T: Into<Bson>>(self, other: T) -> Query {
    Query::Lte(self.into(), other.into())
  }
  /// Checks if `array` contains `self`.
  ///
  /// `{a: {$in: [b, c, d, ...]}}`
  fn is_in<T: Into<Bson>, V: Into<Vec<T>>>(self, array: V) -> Query {
    let bsons = array.into().into_iter().map(T::into).collect::<Vec<_>>();
    Query::In(self.into(), bsons)
  }
  /// Checks if `array` does not contain `self`.
  ///
  /// `{a: {$nin: [b, c, d, ...]}}`
  fn not_in<T: Into<Bson>, V: Into<Vec<T>>>(self, array: V) -> Query {
    let bsons = array.into().into_iter().map(T::into).collect::<Vec<_>>();
    Query::Nin(self.into(), bsons)
  }
  /// Checks if the field of `self` exists in the document. `{a: {$exists: <bool>}}`
  fn exists(self, value: bool) -> Query {
    Query::Exists(self.into(), value)
  }
  /// Filters documents based on the result of the modulo operation
  fn modulo(self, divisor: i32, remainder: i32) -> Query {
    Query::Mod(self.into(), divisor, remainder)
  }
  /// Filters documents matching the regular expression.
  fn regex(self, regex: &bson::Regex) -> Query {
    Query::Regex(self.into(), Bson::RegularExpression(regex.clone()))
  }
  /// Filters documents matching the regular expression.
  ///
  /// `options` is a substring of "imxs", where:
  /// - i: case-insensitive
  /// - m: multiline
  /// - x: ignore whitespace (extended mode)
  /// - s: dot matches newline
  ///
  /// See [MongoDB docs on $regex](https://docs.mongodb.com/manual/reference/operator/query/regex/)
  fn regex_str(self, regex: &str, options: &str) -> Query {
    let regex = bson::Regex {
      pattern: regex.to_owned(),
      options: options.to_owned(),
    };
    Query::Regex(self.into(), Bson::RegularExpression(regex))
  }
}

impl ToQuery for &'static str {}
impl ToQuery for String {}

/// MongoDB query operations.
///
/// The raw enum constructors require wrapping nested Queries in a `Box`,
/// so the idiomatic way to create Queries is to use `ToQuery` methods defined on string literals:
/// ```
/// fn index() {
///   let query: Document = ("fruit".is("apple") & "delicious".is(true)).into();
///   let doc = doc! {
///     "$and": [
///       { "fruit": "apple" },
///       { "delicious": true }
///     ]
///   };
///   assert_eq!(doc, query);
/// }
/// ```
///
/// Outside of the official operations, `Query` provides a few helper constructors:
/// - `Query::id`: construct an ObjectId query
/// - `Query::build`: merge multiple Queries into a single, final filter document
/// - `Query::search`: create a `$text` query without any options
/// - `Query::then`: merge Queries on the same level
#[derive(Clone, Debug)]
pub enum Query {
  /// An empty query.
  Empty,
  /// A wrapper around multiple distinct queries.
  Many(Box<Vec<Query>>),
  /// Joins query clauses with a logical OR returns
  /// all documents that match the conditions of either clause.
  Or(Box<Vec<Query>>),
  /// Joins query clauses with a logical `AND` returns
  /// all documents that match the conditions of both clauses.
  And(Box<Vec<Query>>),
  /// Inverts the effect of a query expression and returns
  /// documents that do not match the query expression.
  Not(String, Box<Query>),
  /// Joins query clauses with a logical `NOR` returns
  /// all documents that fail to match both clauses.
  Nor(Box<Vec<Query>>),
  /// Matches arrays that contain all elements specified in the query.
  All(String, Vec<Bson>),
  /// Matches a value that is identical to a specified value.
  Is(String, Bson),
  /// Matches values that are equal to a specified value.
  Eq(String, Bson),
  /// Matches all values that are not equal to a specified value.
  Ne(String, Bson),
  /// Matches values that are less than a specified value.
  Lt(String, Bson),
  /// Matches values that are less than or equal to a specified value.
  Lte(String, Bson),
  /// Matches values that are greater than a specified value.
  Gt(String, Bson),
  /// Matches values that are greater than or equal to a specified value.
  Gte(String, Bson),
  /// Matches any of the values specified in an array.
  In(String, Vec<Bson>),
  /// Matches none of the values specified in an array.
  Nin(String, Vec<Bson>),
  /// Matches documents that have the specified field.
  Exists(String, bool),
  /// Performs a modulo operation on the value of a field
  /// and selects documents with a specified result.
  Mod(String, i32, i32),
  /// Selects documents where values match a specified regular expression.
  Regex(String, Bson),
  /// Performs text search.
  Text {
    search: String,
    language: Option<String>,
    case_sensitive: Option<bool>,
    diacritic_sensitive: Option<bool>,
  },
  /// Matches documents that satisfy a JavaScript expression.
  Where(String),
  /// Selects documents if the array field is a specified size.
  Size(String, i32),
}

impl Query {
  /// Performs a text search without defining options.
  pub fn search<I: Into<String>>(search: I) -> Query {
    Query::Text {
      search: search.into(),
      language: None,
      case_sensitive: None,
      diacritic_sensitive: None,
    }
  }
  pub fn id<'a>(id: impl Into<&'a str>) -> Result<Document, bson::oid::Error> {
    Ok(bson::doc! {
      "_id": bson::oid::ObjectId::with_string(id.into())?
    })
  }
  /// Returns: `(operation, alt_operation)`
  fn ident(&self) -> (Option<&str>, Option<String>) {
    match self.clone() {
      Query::Or(_) => (Some("$or"), None),
      Query::And(_) => (Some("$and"), None),
      Query::Nor(_) => (Some("$nor"), None),
      Query::Text { .. } => (Some("$text"), None),
      Query::Where(_) => (Some("$where"), None),
      Query::Eq(a, _) => (Some("$eq"), Some(a)),
      Query::Ne(a, _) => (Some("$ne"), Some(a)),
      Query::Lt(a, _) => (Some("$lt"), Some(a)),
      Query::Gt(a, _) => (Some("$gt"), Some(a)),
      Query::In(a, _) => (Some("$in"), Some(a)),
      Query::Lte(a, _) => (Some("$lte"), Some(a)),
      Query::Gte(a, _) => (Some("$gte"), Some(a)),
      Query::Nin(a, _) => (Some("$nin"), Some(a)),
      Query::Not(a, _) => (Some("$not"), Some(a)),
      Query::All(a, _) => (Some("$all"), Some(a)),
      Query::Mod(a, ..) => (Some("$mod"), Some(a)),
      Query::Size(a, _) => (Some("$size"), Some(a)),
      Query::Regex(a, _) => (Some("$regex"), Some(a)),
      Query::Exists(a, _) => (Some("$exists"), Some(a)),
      Query::Empty => (None, None),
      Query::Many(_) => (None, Some(String::new())),
      Query::Is(a, _) => (None, Some(a)),
    }
  }
  /// Scaffold a BSON document from the specified queries.
  pub fn build<I: Into<Vec<Query>>>(queries: I) -> Document {
    let queries: Vec<Query> = queries.into();
    if queries.is_empty() {
      return Document::new();
    }
    let mut query = Query::Empty;
    for q in queries.into_iter() {
      query.then(q);
    }
    query.into()
  }
  pub fn and(self, other: Query) -> Query {
    Query::And(Box::new([self, other.clone()].into()))
  }
  pub fn or(self, other: Query) -> Query {
    Query::Or(Box::new([self, other.clone()].into()))
  }
  pub fn nor(self, other: Query) -> Query {
    Query::Nor(Box::new([self, other.clone()].into()))
  }
  pub fn not(self) -> Query {
    match self.ident() {
      (None, Some(a)) => match a.is_empty() {
        true => {
          if let Query::Many(queries) = self {
            let neo = queries.into_iter().map(|e| e.not()).collect::<Vec<_>>();
            Query::Many(Box::new(neo))
          } else {
            self
          }
        }
        false => {
          if let Query::Is(a, b) = self {
            Query::Not(a.clone(), Box::new(Query::Eq(a, b)))
          } else {
            self
          }
        }
      },
      (Some(_), None) => match self {
        Query::Nor(queries) => Query::Or(queries),
        Query::Or(queries) => Query::Nor(queries),
        Query::And(queries) => {
          let neo = queries.into_iter().map(|e| e.not()).collect::<Vec<_>>();
          Query::Or(Box::new(neo))
        }
        other => unimplemented!("{:?} is unsupported", other),
      },
      (Some(id), Some(_)) => {
        if let Query::Not(_, c) = self {
          c.as_ref().clone()
        } else {
          Query::Not(id.to_owned(), Box::new(self))
        }
      }
      _ => self,
    }
  }
  /// Merge this query with the specified query by performing a deep assignment
  /// if they are the same type, or wrapping in a `Query::Many`.
  pub fn then(&mut self, other: Query) {
    match other {
      Query::Many(queries) => {
        for query in queries.into_iter() {
          self.then(query);
        }
      }
      other => match self {
        Query::Empty => *self = other,
        Query::Many(a) => match other {
          Query::Many(b) => a.extend(b.into_iter()),
          any => a.extend_from_slice(&[any]),
        },
        Query::Or(a) => match other {
          Query::Or(b) => a.extend(b.into_iter()),
          any => a.extend_from_slice(&[any]),
        },
        Query::And(a) => match other {
          Query::And(b) => a.extend(b.into_iter()),
          other => {
            *self = Query::Many(Box::new([Query::And(a.clone()), other].into()));
          }
        },
        Query::Nor(a) => match other {
          Query::Nor(b) => a.extend(b.into_iter()),
          any => a.extend_from_slice(&[any]),
        },
        Query::Not(left, a) => match other {
          Query::Not(right, b) => {
            if *left == *right {
              a.then(b.as_ref().clone());
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Not(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::All(left, a) => match other {
          Query::All(right, b) => {
            if *left == *right {
              a.extend(b.clone().into_iter());
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::All(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Is(left, a) => match other {
          Query::Is(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Is(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Eq(left, a) => match other {
          Query::Eq(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Eq(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Ne(left, a) => match other {
          Query::Ne(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Ne(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Lt(left, a) => match other {
          Query::Lt(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Lt(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Lte(left, a) => match other {
          Query::Lte(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Lte(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Gt(left, a) => match other {
          Query::Gt(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Gt(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Gte(left, a) => match other {
          Query::Gte(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Gte(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::In(left, a) => match other {
          Query::In(right, b) => {
            if *left == *right {
              a.extend(b);
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::In(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Nin(left, a) => match other {
          Query::Nin(right, b) => {
            if *left == *right {
              a.extend(b);
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Nin(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Exists(left, a) => match other {
          Query::Exists(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Exists(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Mod(left, aa, ab) => match other {
          Query::Mod(right, ba, bb) => {
            if *left == *right {
              *aa = ba;
              *ab = bb;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Mod(right, ba, bb)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Regex(left, a) => match other {
          Query::Regex(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Regex(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Text {
          search: aa,
          language: ab,
          case_sensitive: ac,
          diacritic_sensitive: ad,
        } => match other {
          Query::Text {
            search: ba,
            language: bb,
            case_sensitive: bc,
            diacritic_sensitive: bd,
          } => {
            *aa = ba;
            *ab = bb;
            *ac = bc;
            *ad = bd;
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Where(a) => match other {
          Query::Where(b) => {
            *a = b;
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
        Query::Size(left, a) => match other {
          Query::Size(right, b) => {
            if *left == *right {
              *a = b;
            } else {
              *self = Query::Many(Box::new(vec![self.clone(), Query::Size(right, b)]));
            }
          }
          other => {
            *self = Query::Many(Box::new([self.clone(), other].into()));
          }
        },
      },
    }
  }
}

impl From<Query> for Bson {
  fn from(query: Query) -> Self {
    match query {
      Query::Empty => Bson::Null,
      Query::Many(a) | Query::Or(a) | Query::And(a) | Query::Nor(a) => Bson::Array(
        a.into_iter()
          .map(|e| Bson::Document(e.into()))
          .collect::<Vec<_>>(),
      ),
      Query::Not(_, a) => a.as_ref().into(),
      Query::All(_, a) | Query::In(_, a) | Query::Nin(_, a) => Bson::Array(a),
      Query::Is(_, a)
      | Query::Eq(_, a)
      | Query::Ne(_, a)
      | Query::Lt(_, a)
      | Query::Lte(_, a)
      | Query::Gt(_, a)
      | Query::Regex(_, a)
      | Query::Gte(_, a) => a,
      Query::Exists(_, a) => Bson::Boolean(a),
      Query::Mod(_, a, b) => bson::bson!([a, b]),
      Query::Text {
        search,
        language,
        case_sensitive,
        diacritic_sensitive,
      } => {
        let mut doc = Document::new();
        doc.insert("search", search);
        if let Some(lang) = language {
          doc.insert("$language", lang);
        }
        if let Some(case) = case_sensitive {
          doc.insert("$caseSensitive", case);
        }
        if let Some(dia) = diacritic_sensitive {
          doc.insert("$diacriticSensitive", dia);
        }
        Bson::Document(doc)
      }
      Query::Where(a) => Bson::JavaScriptCode(a),
      Query::Size(_, a) => Bson::Int32(a),
    }
  }
}

impl From<Query> for Document {
  fn from(query: Query) -> Document {
    let mut doc = Document::new();
    match query {
      Query::Or(a) => {
        let docs = a.into_iter().map(|e| Bson::Document(e.into())).collect();
        doc.insert("$or", Bson::Array(docs));
      }
      Query::And(a) => {
        let docs = a.into_iter().map(|e| Bson::Document(e.into())).collect();
        doc.insert("$and", Bson::Array(docs));
      }
      Query::Not(id, query) => {
        let bson = Document::from(query.as_ref().clone());
        let inner = bson.get_document(&id).unwrap().clone();
        doc.insert(id, doc! { "$not": Bson::Document(inner) });
      }
      Query::Nor(a) => {
        let docs = a.into_iter().map(|e| Bson::Document(e.into())).collect();
        doc.insert("$nor", Bson::Array(docs));
      }
      Query::All(a, b) => {
        doc.insert(a, doc! { "$all": b });
      }
      Query::Text {
        search,
        language,
        case_sensitive,
        diacritic_sensitive,
      } => {
        let mut temp = Document::new();
        temp.insert("search", search);
        if let Some(lang) = language {
          temp.insert("$language", lang);
        }
        if let Some(case) = case_sensitive {
          temp.insert("$caseSensitive", case);
        }
        if let Some(dia) = diacritic_sensitive {
          temp.insert("$diacriticSensitive", dia);
        }
        doc.insert("$text", Bson::Document(temp));
      }
      Query::Empty => {}
      Query::Many(queries) => {
        for query in queries.iter() {
          match query.ident() {
            (Some(id), Some(a)) => {
              let bson: Bson = query.into();
              doc.insert(a, Bson::Document(doc! { id: bson }));
            }
            (Some(id), None) => {
              let bson: Bson = query.into();
              doc.insert(id, bson);
            }
            (None, Some(a)) => {
              let bson: Bson = query.into();
              doc.insert(a, bson);
            }
            _ => {}
          }
        }
      }
      Query::Is(a, b) => {
        doc.insert(a, b);
      }
      Query::Eq(a, b) => {
        doc.insert(a, doc! { "$eq": b });
      }
      Query::Ne(a, b) => {
        doc.insert(a, doc! { "$ne": b });
      }
      Query::Lt(a, b) => {
        doc.insert(a, doc! { "$lt": b });
      }
      Query::Lte(a, b) => {
        doc.insert(a, doc! { "$lte": b });
      }
      Query::Gt(a, b) => {
        doc.insert(a, doc! { "$gt": b });
      }
      Query::Gte(a, b) => {
        doc.insert(a, doc! { "$gte": b });
      }
      Query::In(a, b) => {
        doc.insert(a, doc! { "$in": b });
      }
      Query::Nin(a, b) => {
        doc.insert(a, doc! { "$nin": b });
      }
      Query::Exists(a, b) => {
        doc.insert(a, doc! { "$exists": b });
      }
      Query::Mod(a, b, c) => {
        doc.insert(a, doc! { "$mod":  [ b, c ] });
      }
      Query::Regex(a, b) => {
        doc.insert(a, doc! { "$regex": b });
      }
      Query::Where(a) => {
        doc.insert("$where", a);
      }
      Query::Size(a, b) => {
        doc.insert(a, doc! { "$size": b });
      }
    }
    doc
  }
}

impl ops::BitAnd for Query {
  type Output = Query;

  fn bitand(self, rhs: Query) -> Self::Output {
    self.and(rhs)
  }
}

impl ops::BitOr for Query {
  type Output = Query;

  fn bitor(self, rhs: Self) -> Self::Output {
    self.or(rhs)
  }
}

impl ops::Not for Query {
  type Output = Query;

  fn not(self) -> Self::Output {
    self.not()
  }
}
