pub mod query;
pub use cascade::cascade;

#[cfg(test)]
mod tests {
  use crate::query::{Query, ToQuery};
  use crate::{cascade, f};
  use bson::Document;

  struct Article {
    title: String,
    content: String,
    author: String,
    published: bool,
  }

  #[test]
  fn field_verification() {
    let a = Query::build([
      f!(published in Article).exists(true),
      f!(author in Article).exists(false),
    ]);
    let doc = bson::doc! {
      "published": {
        "$exists": true
      },
      "author": {
        "$exists": false
      }
    };
    assert_eq!(doc, a);
  }

  #[test]
  fn adhoc_vs_builder() {
    // All of these methods yield the same final query document
    // Ad-hoc building, use the `cascade` macro for more ergonomic querying
    let query = cascade! {
      "first".is(true) & "success".exists(false);
      ..then("what".is("going_on"));
      ..then(Query::search("Search for this"));
      ..then("powerful".is(true));
      ..then("action".is("try_again"));
      ..then(!"some".is("what"));
    };

    // Builder function, returns the final document
    let builder = Query::build([
      "first".is(true) & "success".exists(false),
      "what".is("going_on"),
      Query::search("Search for this"),
      "powerful".is(true),
      "action".is("try_again"),
      !"some".is("what"),
    ]);

    // doc! macro
    let doc = bson::doc! {
      "$and": [
        { "first": true },
        { "success": { "$exists": false } }
      ],
      "what": "going_on",
      "$text": {
        "search": "Search for this"
      },
      "powerful": true,
      "action": "try_again",
      "some": {
        "$ne": "what"
      }
    };

    let ad_hoc: Document = query.into();
    assert_eq!(
      ad_hoc, builder,
      "Ad hoc building matches builder:\n\n{}\nshould equal\n{}\n\n",
      ad_hoc, builder
    );
    assert_eq!(
      builder, doc,
      "Builder matches official specs:\n\n{}\nshould equal\n{}\n\n",
      builder, doc
    );
  }
}
