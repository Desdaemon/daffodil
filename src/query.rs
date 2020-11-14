use bson::{doc, Bson, Document};

/// Convenience methods to construct Queries from `&str` and `String`.
///
/// Example:
/// ```
/// fn index() {
///   let filter = "author".is_in(["Author A", "Author B", ...]);
///   filter.then("uploaded_at".more_than(""))
/// }
/// ```
pub trait ToQuery<'a>
where
  Self: Into<&'a str>,
{
  /// Equivalence. `{a: b}`
  fn is<I: Into<Bson>>(self, other: I) -> Query<'a> {
    Query::Is(self.into(), other.into())
  }
  /// Non-equivalence. `{a: {$ne: b}}`
  fn not<T: Into<Bson>>(self, other: T) -> Query<'a> {
    Query::Ne(self.into(), other.into())
  }
  /// Greater than. `{a: {$gt: b}}`
  fn more_than<T: Into<Bson>>(self, other: T) -> Query<'a> {
    Query::Gt(self.into(), other.into())
  }
  /// Greater than or equal. `{a: {$gte: b}}`
  fn gte<T: Into<Bson>>(self, other: T) -> Query<'a> {
    Query::Gte(self.into(), other.into())
  }
  /// Less than. `{a: {$lt: b}}`
  fn less_than<T: Into<Bson>>(self, other: T) -> Query<'a> {
    Query::Lt(self.into(), other.into())
  }
  /// Less than or equal. `{a: {$lte: b}}`
  fn lte<T: Into<Bson>>(self, other: T) -> Query<'a> {
    Query::Lte(self.into(), other.into())
  }
  /// Checks if `array` contains `self`.
  ///
  /// `{a: {$in: [b, c, d, ...]}}`
  fn is_in<T: Into<Bson> + Copy>(self, array: &[T]) -> Query<'a> {
    let bsons = array.iter().map(|e| (*e).into()).collect::<Vec<_>>();
    Query::In(self.into(), bsons)
  }
  /// Checks if `array` does not contain `self`.
  ///
  /// `{a: {$nin: [b, c, d, ...]}}`
  fn not_in<T: Into<Bson> + Copy>(self, array: &[T]) -> Query<'a> {
    let bsons = array.iter().map(|e| (*e).into()).collect::<Vec<_>>();
    Query::Nin(self.into(), bsons)
  }
  /// Checks if the field of `self` exists in the document. `{a: {$exists: <bool>}}`
  fn exists(self, value: bool) -> Query<'a> {
    Query::Exists(self.into(), value)
  }
  /// Filters documents based on the result of the modulo operation
  fn modulo(self, divisor: i32, remainder: i32) -> Query<'a> {
    Query::Mod(self.into(), divisor, remainder)
  }
  /// Filters documents matching the regular expression.
  fn regex_from(self, regex: bson::Regex) -> Query<'a> {
    Query::Regex(self.into(), Bson::RegularExpression(regex))
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
  fn regex(self, regex: &str, options: &str) -> Query<'a> {
    let regex = bson::Regex {
      pattern: regex.to_owned(),
      options: options.to_owned(),
    };
    Query::Regex(self.into(), Bson::RegularExpression(regex))
  }
  /// Match this field to a list of ObjectIds
  fn id_in(self, ids: &[bson::oid::ObjectId]) -> Query<'a> {
    Query::In(
      self.into(),
      ids
        .into_iter()
        .map(|e| Bson::ObjectId(e.to_owned()))
        .collect(),
    )
  }
}

impl<'a> ToQuery<'a> for &'a str {}

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
pub enum Query<'a> {
  /// An empty query.
  Empty,
  /// A wrapper around multiple distinct queries.
  Many(Vec<Query<'a>>),
  /// Joins query clauses with a logical OR returns
  /// all documents that match the conditions of either clause.
  Or(Vec<Query<'a>>),
  /// Joins query clauses with a logical `AND` returns
  /// all documents that match the conditions of both clauses.
  And(Vec<Query<'a>>),
  /// Inverts the effect of a query expression and returns
  /// documents that do not match the query expression.
  Not(&'a str, Box<Query<'a>>),
  /// Joins query clauses with a logical `NOR` returns
  /// all documents that fail to match both clauses.
  Nor(Vec<Query<'a>>),
  /// Matches arrays that contain all elements specified in the query.
  All(&'a str, Vec<Bson>),
  /// Matches a value that is identical to a specified value.
  Is(&'a str, Bson),
  /// Matches values that are equal to a specified value.
  Eq(&'a str, Bson),
  /// Matches all values that are not equal to a specified value.
  Ne(&'a str, Bson),
  /// Matches values that are less than a specified value.
  Lt(&'a str, Bson),
  /// Matches values that are less than or equal to a specified value.
  Lte(&'a str, Bson),
  /// Matches values that are greater than a specified value.
  Gt(&'a str, Bson),
  /// Matches values that are greater than or equal to a specified value.
  Gte(&'a str, Bson),
  /// Matches any of the values specified in an array.
  In(&'a str, Vec<Bson>),
  /// Matches none of the values specified in an array.
  Nin(&'a str, Vec<Bson>),
  /// Matches documents that have the specified field.
  Exists(&'a str, bool),
  /// Performs a modulo operation on the value of a field
  /// and selects documents with a specified result.
  Mod(&'a str, i32, i32),
  /// Selects documents where values match a specified regular expression.
  Regex(&'a str, Bson),
  /// Performs text search.
  Text {
    search: &'a str,
    language: Option<&'a str>,
    case_sensitive: Option<bool>,
    diacritic_sensitive: Option<bool>,
  },
  /// Matches documents that satisfy a JavaScript expression.
  Where(&'a str),
  /// Selects documents if the array field is a specified size.
  Size(&'a str, i32),
}

impl<'a> Query<'a> {
  /// Performs a text search without defining options.
  pub fn search(search: &'a str) -> Query<'a> {
    Query::Text {
      search,
      language: None,
      case_sensitive: None,
      diacritic_sensitive: None,
    }
  }
  /// Create an ObjectId query, optionally with a key
  pub fn id(id: impl Into<&'a str>, key: Option<&str>) -> Result<Query, bson::oid::Error> {
    Ok(
      key
        .unwrap_or("_id")
        .is(bson::oid::ObjectId::with_string(id.into())?),
    )
  }
  /// Returns: `(operation, alt_operation)`
  fn ident(&self) -> (Option<&str>, Option<&str>) {
    match self {
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
      Query::Many(_) => (None, Some("")),
      Query::Is(a, _) => (None, Some(a)),
    }
  }
  /// Scaffold a BSON document from the specified queries.
  pub fn build(queries: &[Query]) -> Document {
    let queries: Vec<Query> = queries.into();
    if queries.is_empty() {
      return Document::new();
    }
    queries
      .into_iter()
      .fold(Query::Empty, |p, n| p.then(n))
      .into()
  }
  pub fn and(self, other: Query<'a>) -> Query {
    Query::And(vec![self, other])
  }
  pub fn or(self, other: Query<'a>) -> Query {
    Query::Or(vec![self, other])
  }
  pub fn nor(self, other: Query<'a>) -> Query {
    Query::Nor(vec![self, other])
  }
  /// Map this query to its logical negation, or wrap in a $not where not possible
  pub fn not(self) -> Query<'a> {
    match self {
      Query::Empty => self,
      Query::Many(a) => Query::Many(a.into_iter().map(|e| e.not()).collect()),
      Query::Or(a) => Query::Nor(a),
      Query::And(a) => Query::Or(a.into_iter().map(|e| e.not()).collect()),
      Query::Not(_, a) => a.as_ref().to_owned(),
      Query::Nor(a) => Query::Or(a),
      Query::Is(a, b) | Query::Eq(a, b) => Query::Ne(a, b),
      Query::Ne(a, b) => Query::Is(a, b),
      Query::Lt(a, b) => Query::Gte(a, b),
      Query::Lte(a, b) => Query::Gt(a, b),
      Query::Gt(a, b) => Query::Lte(a, b),
      Query::Gte(a, b) => Query::Lt(a, b),
      Query::In(a, b) => Query::Nin(a, b),
      Query::Nin(a, b) => Query::In(a, b),
      Query::Exists(a, b) => Query::Exists(a, !b),
      Query::Mod(a, b, c) => Query::Not(a, Box::new(Query::Mod(a, b, c))),
      Query::Regex(..)
      | Query::Text { .. }
      | Query::Where(..)
      | Query::Size(..)
      | Query::All(..) => unimplemented!("{:?} is uninplemented", self),
    }
  }

  /// Merge this query with the specified query by performing a deep assignment
  /// if they are the same type, or wrapping in a `Query::Many`.
  pub fn then(mut self, other: Query<'a>) -> Query<'a> {
    let same_ident = { self.ident() == other.ident() };
    self = match (self, other, same_ident) {
      (any, Query::Many(b), _) => match any {
        Query::Many(a) => Query::Many(vec![a, b].concat()),
        any => b.into_iter().fold(any, |p, n| p.then(n)),
      },
      // Overwrite
      (Query::Empty, other, _) => other,
      (
        Query::Text { .. },
        Query::Text {
          search,
          language,
          case_sensitive,
          diacritic_sensitive,
        },
        _,
      ) => Query::Text {
        search,
        language,
        case_sensitive,
        diacritic_sensitive,
      },
      (Query::Where(_), Query::Where(a), _) => Query::Where(a),
      // Inner concat
      (Query::Or(a), Query::Or(b), _) => Query::Or(vec![a, b].concat()),
      (Query::And(a), Query::And(b), _) => Query::And(vec![a, b].concat()),
      (Query::Nor(a), Query::Nor(b), _) => Query::Nor(vec![a, b].concat()),
      (Query::Not(a, i), Query::Not(_, j), true) => {
        Query::Not(a, Box::new(i.then(j.as_ref().to_owned())))
      }
      // Matching concat
      (Query::All(a, i), Query::All(_, j), true) => Query::All(a, vec![i, j].concat()),
      (Query::In(a, i), Query::In(_, j), true) => Query::In(a, vec![i, j].concat()),
      (Query::Nin(a, i), Query::Nin(_, j), true) => Query::Nin(a, vec![i, j].concat()),
      // Matching overwrite
      (Query::Is(a, _), Query::Is(_, j), true) => Query::Is(a, j),
      (Query::Eq(a, _), Query::Eq(_, j), true) => Query::Eq(a, j),
      (Query::Ne(a, _), Query::Ne(_, j), true) => Query::Ne(a, j),
      (Query::Lt(a, _), Query::Lt(_, j), true) => Query::Lt(a, j),
      (Query::Lte(a, _), Query::Lte(_, j), true) => Query::Lte(a, j),
      (Query::Gt(a, _), Query::Gt(_, j), true) => Query::Gt(a, j),
      (Query::Gte(a, _), Query::Gte(_, j), true) => Query::Gte(a, j),
      (Query::Exists(a, _), Query::Exists(_, j), true) => Query::Exists(a, j),
      (Query::Mod(a, ..), Query::Mod(_, e, f), true) => Query::Mod(a, e, f),
      (Query::Regex(a, _), Query::Regex(_, j), true) => Query::Regex(a, j),
      (Query::Size(a, _), Query::Size(_, j), true) => Query::Size(a, j),
      (any, other, _) => {
        self = Query::Many(vec![any, other]);
        self
      }
    };
    self
  }
}

impl<'a> From<Query<'a>> for Bson {
  fn from(query: Query) -> Self {
    match query {
      Query::Empty => Bson::Null,
      Query::Many(a) | Query::Or(a) | Query::And(a) | Query::Nor(a) => Bson::Array(
        a.into_iter()
          .map(|e| Bson::Document(e.into()))
          .collect::<Vec<_>>(),
      ),
      Query::Not(_, a) => a.as_ref().into(),
      Query::All(_, a) | Query::In(_, a) | Query::Nin(_, a) => Bson::Array(a.into()),
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
      Query::Where(a) => Bson::JavaScriptCode(a.into()),
      Query::Size(_, a) => Bson::Int32(a),
    }
  }
}

impl<'a> From<Query<'a>> for Document {
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
        let bson: Document = query.as_ref().to_owned().into();
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
