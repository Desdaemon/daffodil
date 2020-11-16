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
  /// Internal method, use `Self::with::<Model>()` instead.
  #[doc(hidden)]
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
    other: &Option<Foreign>,
    options: Option<UpdateOptions>,
  ) -> wither::Result<wither::mongodb::results::UpdateResult>;

  /// Internal method, use `self.delete_rel::<Model>(..)` instead.
  #[doc(hidden)]
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
/// let pipeline = Movie::with::<Actor>();
/// let movies = aggregate!(&db, pipeline: pipeline, Movie);
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
    &mut self,
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
pub trait ModelsExt<T> {
  /// Save all Models in this list.
  async fn save(&mut self, db: &Database, filter: Option<Vec<Document>>) -> wither::Result<()>;

  async fn save_rels(
    &mut self,
    db: &Database,
    filter: Option<Vec<Document>>,
    options: Option<UpdateOptions>,
  ) -> wither::Result<()>
  where
    T: RelationsAll + Clone;

  async fn persist(mut self, db: &Database) -> wither::Result<Self>
  where
    Self: Sized;

  async fn persist_with(mut self, db: &Database) -> wither::Result<Self>
  where
    Self: Sized,
    T: RelationsAll + Clone;

  /// Delete all Models in this list.
  async fn delete(
    &self,
    db: &Database,
    options: Option<DeleteOptions>,
  ) -> wither::Result<DeleteResult>;
}

#[async_trait]
impl<T: Model + Send + Sync> ModelsExt<T> for [T] {
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

  async fn persist(mut self, db: &Database) -> wither::Result<Self>
  where
    Self: Sized,
  {
    self.save(db, None).await?;
    Ok(self)
  }

  async fn persist_with(mut self, db: &Database) -> wither::Result<Self>
  where
    Self: Sized,
    T: RelationsAll + Clone,
  {
    self.save_rels(db, None, None).await?;
    Ok(self)
  }

  async fn save_rels(
    &mut self,
    db: &Database,
    filter: Option<Vec<Document>>,
    options: Option<UpdateOptions>,
  ) -> wither::Result<()>
  where
    T: RelationsAll + Clone,
  {
    if let Some(filter) = filter {
      for (item, filter) in self.iter_mut().zip(filter) {
        item.save_rels(db, Some(filter), options.clone()).await?;
      }
    } else {
      for item in self.iter_mut() {
        item.save_rels(db, None, options.clone()).await?;
      }
    }
    Ok(())
  }
}

#[async_trait]
impl<T: Model + Send + Sync> ModelsExt<T> for Vec<T> {
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

  async fn save_rels(
    &mut self,
    db: &Database,
    filter: Option<Vec<Document>>,
    options: Option<UpdateOptions>,
  ) -> wither::Result<()>
  where
    T: RelationsAll + Clone,
  {
    self[..].save_rels(db, filter, options).await
  }

  async fn persist(mut self, db: &Database) -> wither::Result<Self>
  where
    Self: Sized,
  {
    todo!()
  }

  async fn persist_with(mut self, db: &Database) -> wither::Result<Self>
  where
    Self: Sized,
    T: RelationsAll + Clone,
  {
    todo!()
  }
}

#[async_trait]
pub trait ModelExt
where
  Self: Model,
{
  /// The inline version of `save()`, meant for creating new documents.
  async fn persist(mut self, db: &Database) -> wither::Result<Self> {
    self.save(db, None).await?;
    Ok(self)
  }

  /// The inline version of `save_rels()`, meant for creating new documents.
  ///
  /// *Example usage:*
  /// ```
  /// let director = Person {
  ///   name: "Titanic Director".into(),
  ///   ..Person::default()
  /// }
  /// .persist_with(db)
  /// .await?;
  /// ```
  async fn persist_with(mut self, db: &Database) -> wither::Result<Self>
  where
    Self: RelationsAll + Clone + Sync,
  {
    self.save_rels(db, None, None).await?;
    Ok(self)
  }
}

impl<T: Model> ModelExt for T {}
