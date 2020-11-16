use wither::{
  bson::Bson,
  mongodb::{
    options::{DeleteOptions, UpdateOptions},
    results::DeleteResult,
  },
};

use crate::prelude::*;

/// The base on which relational mappings for MongoDB collections are defined.
#[async_trait]
pub trait Relations<This: ?Sized, Foreign> {
  /// Private method, use `Self::with::<Model>()` instead.
  fn _with<L: crate::private::IsLocal>() -> Vec<Document>;

  /// Updates the model's relations on the database using the specified
  /// `Foreign` document. This does not affect the local Model.
  ///
  /// **Failure**  
  /// Throws an error when either `self` or `other` contains a document without an ObjectId.
  /// For this reason, it is recommended to persist `self` and `other` before running this function.
  async fn save_rel(
    &self,
    db: &Database,
    other: Option<Foreign>,
    options: Option<UpdateOptions>,
  ) -> wither::Result<wither::mongodb::results::UpdateResult>;

  /// Private method, use `self.delete_rel::<Model>(..)` instead.
  async fn _delete_rel<L: crate::private::IsLocal>(
    &self,
    db: &Database,
  ) -> wither::Result<wither::mongodb::results::DeleteResult>;
}

/// This trait exposes a single method from `Relations`, `with()`
/// which is meant for general consumption.
///
/// *Example usage*:
/// ```
///   let pipeline = Movie::with::<Actor>();
///   let movies = aggregate!(&db, pipeline: pipeline, Movie);
/// ```
#[async_trait]
pub trait With {
  fn with<T>() -> Vec<Document>
  where
    Self: Relations<Self, T>,
  {
    Self::_with::<crate::private::Local>()
  }
  async fn delete_rel<T>(
    &self,
    db: &Database,
  ) -> wither::Result<wither::mongodb::results::DeleteResult>
  where
    Self: Relations<Self, T>,
  {
    Self::_delete_rel::<crate::private::Local>(self, db).await
  }
}

impl<T: Model> With for T {}

pub trait Timestamps {
  /// Returns update documents for timestamps not marked with `once`
  fn timestamps() -> Bson;

  /// Returns update documents for all timestamps
  fn timestamps_new() -> Bson;
}

/// Functions related to inclusion of all relations in aggregations.
#[async_trait]
pub trait RelationsAll {
  /// Includes all aggregation stages associated with the Model's relations.
  ///
  /// Use this over `with()` when you just need the model and its dependents.
  fn with_all() -> Vec<Document>;

  /// Saves `self` and updates the model's relational data.
  ///
  /// **Params**
  /// - `filter`: The filter document for saving `self`.
  /// - `options`: Options for saving relations.
  async fn save_rels(
    &self,
    db: &Database,
    filter: Option<Document>,
    options: Option<UpdateOptions>,
  ) -> wither::Result<()>
  where
    Self: Clone;

  /// Delete dependencies marked as `cascade` and deletes `self`.
  async fn delete_rels(
    &self,
    db: &Database,
  ) -> wither::Result<Vec<wither::mongodb::results::DeleteResult>>;
}

/// Extensions on Model vectors/iterators.
#[async_trait]
pub trait ModelsExt {
  /// Save all Models in this list.
  async fn save(&mut self, db: &Database, filter: Option<Vec<Document>>) -> wither::Result<()>;

  /// Delete all Models in this list.
  async fn delete(
    &self,
    db: &Database,
    options: Option<DeleteOptions>,
  ) -> wither::Result<DeleteResult>;
}

#[async_trait]
impl<T: Model + Send + Sync> ModelsExt for [T] {
  async fn save(&mut self, db: &Database, filter: Option<Vec<Document>>) -> wither::Result<()> {
    if let Some(docs) = filter {
      for (item, filter) in self.iter_mut().zip(docs) {
        item.save(db, Some(filter)).await?;
      }
    } else {
      for item in self.iter_mut() {
        item.save(db, None).await?;
      }
    }
    Ok(())
  }

  async fn delete(
    &self,
    db: &Database,
    options: Option<DeleteOptions>,
  ) -> wither::Result<DeleteResult> {
    let ids = self.iter().filter_map(|e| e.id()).collect::<Vec<_>>();
    Ok(
      T::collection(db)
        .delete_many(doc! { "_id": { "$in": ids }}, options)
        .await?,
    )
  }
}

#[async_trait]
impl<T: Model + Send + Sync> ModelsExt for Vec<T> {
  async fn save(&mut self, db: &Database, filter: Option<Vec<Document>>) -> wither::Result<()> {
    self[..].save(db, filter).await
  }

  async fn delete(
    &self,
    db: &Database,
    options: Option<DeleteOptions>,
  ) -> wither::Result<DeleteResult> {
    self[..].delete(db, options).await
  }
}
