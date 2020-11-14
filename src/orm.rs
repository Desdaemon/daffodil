use wither::mongodb::{options::AggregateOptions, options::UpdateOptions, results::DeleteResult};

use crate::prelude::*;

/// The base on which relational mappings for MongoDB collections are defined.
#[async_trait]
pub trait Relations<This: ?Sized, Foreign> {
  /// Prepares aggregation stages to join to other collections.
  /// Do not manually implement this, instead use `#[derive(Orm)]` on your models.
  ///
  /// It is preferred to use `Self::with::<Foreign>()` whenever possible.
  fn _with() -> Vec<Document>;

  /// Updates the model's relations on the database using the specified
  /// `Foreign` document. This does not affect the local Model.
  ///
  /// # Failure
  /// Throws an error when either `self` or `other` contains a document without an ObjectId.
  /// For this reason, it is recommended to persist the `self` and `other` before running this function.
  async fn save_rel(
    &self,
    db: &Database,
    other: &Foreign,
    options: Option<UpdateOptions>,
  ) -> wither::Result<wither::mongodb::results::UpdateResult>;
}

/// This trait exposes a single method from `Relations`, `with()`
/// which is meant for general consumption.
///
/// *Example usage*:
/// ```
///   let pipeline = Movie::with::<Actor>();
///   let movies = aggregate!(&db, pipeline: pipeline, Movie);
/// ```
pub trait With {
  fn with<T>() -> Vec<Document>
  where
    Self: Relations<Self, T>,
  {
    Self::_with()
  }
}

impl<T> With for T {}

/// Functions related to inclusion of all relations in aggregations.
#[async_trait]
pub trait RelationsAll {
  /// Includes all aggregation stages associated with the Model's relations.
  ///
  /// Use this over `with()` when you just need the model and its dependents.
  fn with_all() -> Vec<Document>;

  /// Updates all relations to the database using data from `self`. The complete version of `save_rel()`.
  async fn save_rels(&self, db: &Database, options: Option<UpdateOptions>) -> wither::Result<()>;
}

/// Quickhand for creating an aggregation pipeline.
/// Only callable inside async functions returning `wither::Result`.
/// Returns `Vec<T>`.
/// ```
/// let result = aggregate!(
///   db,       // `&mongodb::Database`, required.
///   None,     // `Vec<Document>` of aggregation pipeline stages, optional.
///   None,     // `Vec<mongodb::options::AggregateOptions>`, optional.
///   Source,   // `Model` type, required.
///   // flat_map callback: Fn(Document) -> Option<T>, optional.
///   |e| async move { T::instance_from_document(e).ok() },
///   Target,   // Return type, Document by default, or inferred from the callback.
/// );
///
/// let result_no_options = aggregate!(db, pipeline: .., Model);
/// let result_no_pipeline = aggregate!(db, options: .., Model);
/// ```
#[macro_export]
macro_rules! aggregate {
  ($db:expr, $pipeline:expr, $options:expr, $input:ty, $func:expr, $type:ty) => {
    <$input>::collection($db)
      .aggregate($pipeline, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<$type>>()
      .await
  };
  ($db:expr, pipeline: $pipeline:expr, $input:ty, $func:expr, $type:ty) => {
    <$input>::collection($db)
      .aggregate($pipeline, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<$type>>()
      .await
  };
  ($db:expr, options: $options:expr, $input:ty, $func:expr, $type:ty) => {
    <$input>::collection($db)
      .aggregate(None, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<$type>>()
      .await
  };

  ($db:expr, $pipeline:expr, $options:expr, $input:ty, $func:expr) => {
    <$input>::collection($db)
      .aggregate($pipeline, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<_>>()
      .await
  };
  ($db:expr, pipeline: $pipeline:expr, $input:ty, $func:expr) => {
    <$input>::collection($db)
      .aggregate($pipeline, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<_>>()
      .await
  };
  ($db:expr, options: $options:expr, $input:ty, $func:expr) => {
    <$input>::collection($db)
      .aggregate(None, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<_>>()
      .await
  };

  ($db:expr, $pipeline:expr, $options:expr, $input:ty, $type:ty) => {
    <$input>::collection($db)
      .aggregate($pipeline, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .map(|e| async move { <$type>::from(e) })
      .collect::<Vec<$type>>()
      .await
  };
  ($db:expr, pipeline: $pipeline:expr, $input:ty, $type:ty) => {
    <$input>::collection($db)
      .aggregate($pipeline, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .map(|e| async move { <$type>::from(e) })
      .collect::<Vec<$type>>()
      .await
  };
  ($db:expr, options: $options:expr, $input:ty, $type:ty) => {
    <$input>::collection($db)
      .aggregate(None, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .map(|e| async move { <$type>::from(e) })
      .collect::<Vec<$type>>()
      .await
  };

  ($db:expr, $pipeline:expr, $options:expr, $input:ty) => {
    <$input>::collection($db)
      .aggregate($pipeline, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .collect::<Vec<_>>()
      .await
  };
  ($db:expr, pipeline: $pipeline:expr, $input:ty) => {
    <$input>::collection($db)
      .aggregate($pipeline, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .collect::<Vec<_>>()
      .await
  };
  ($db:expr, options: $options:expr, $input:ty) => {
    <$input>::collection($db)
      .aggregate(None, $options)
      .await?
      .filter_map(|e| async move { e.ok() })
      .collect::<Vec<_>>()
      .await
  };

  ($db:expr, $input:ty, $func:expr, $type:ty) => {
    <$input>::collection($db)
      .aggregate(None, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<$type>>()
      .await
  };
  ($db:expr, $input:ty, $func:expr) => {
    <$input>::collection($db)
      .aggregate(None, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .filter_map($func)
      .collect::<Vec<_>>()
      .await
  };
  ($db:expr, $input:ty, $type:ty) => {
    <$input>::collection($db)
      .aggregate(None, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .map(|e| async move { <$type>::from(e) })
      .collect::<Vec<$type>>()
      .await
  };
  ($db:expr, $input:ty) => {
    <$input>::collection($db)
      .aggregate(None, None)
      .await?
      .filter_map(|e| async move { e.ok() })
      .collect::<Vec<_>>()
      .await
  };
}

/// Extension on Models to make use of the aggregation pipeline,
/// which is essential to collection lookups.
#[async_trait]
pub trait Aggregate
where
  Self: Model,
{
  /// Create an aggregation pipeline returning Models.
  async fn aggregate(
    db: &Database,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
  ) -> wither::Result<Vec<Self>> {
    Ok(aggregate!(db, pipeline, options, Self, |e| async move {
      Self::instance_from_document(e).ok()
    }))
  }

  /// Create an aggregation pipeline returning MongoDB documents.
  /// Useful for when you only need it to pass the items as-is.
  async fn aggregate_raw(
    db: &Database,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
  ) -> wither::Result<Vec<Document>> {
    Ok(aggregate!(db, pipeline, options, Self))
  }

  /// Create an aggregation pipeline returning MongoDB documents,
  /// looking up all available relations in the process.
  async fn aggregate_raw_with_all(
    db: &Database,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
  ) -> wither::Result<Vec<Document>>
  where
    Self: RelationsAll,
  {
    let mut pipeline = pipeline;
    pipeline.extend(Self::with_all());
    Ok(aggregate!(db, pipeline, options, Self))
  }
}

impl<T: Model> Aggregate for T {}

/// Extensions on Model vectors/iterators.
#[async_trait]
pub trait ModelsExt {
  /// Save all Models in this list.
  async fn save(&mut self, db: &Database, filter: Option<Vec<Document>>) -> wither::Result<()>;

  /// Delete all Models in this list.
  async fn delete(
    &self,
    db: &Database,
    options: Option<wither::mongodb::options::DeleteOptions>,
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
    options: Option<wither::mongodb::options::DeleteOptions>,
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
    options: Option<wither::mongodb::options::DeleteOptions>,
  ) -> wither::Result<DeleteResult> {
    self[..].delete(db, options).await
  }
}
