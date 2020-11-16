use std::marker::PhantomData;

use async_trait::async_trait;
use futures::StreamExt;
use wither::mongodb::{bson::Document, Database};
use wither::{mongodb::options::AggregateOptions, Model};

use crate::{
  aggregate,
  orm::{Relations, RelationsAll, With},
};

/// An aggregation pipeline in MongoDB.
///
/// *Example usage*:
///
/// Given the following Model:
/// ```
/// #[derive(Model, Relations, Serialize, Deserialize)]
/// struct Product {
///     ...
///     name: String,
///     #[serde(skip_serializing_if = "Option::is_none")]
///     #[has_many]
///     buyers: Option<Vec<Customer>>
/// }
/// ```
/// We can query Products and include Customers from another collection:
/// ```
/// let products = Product::filter(vec![doc! { "$match": { "name": "Computer" } }])
///     .with::<Customer>()
///     .aggregate(db)
///     .await?;
/// ```
/// Alternatively, if your model contains many children/parent relations,
/// you should use `with_all()`.
pub struct AggregatePipeline<T>(Vec<Document>, Option<AggregateOptions>, PhantomData<T>);

impl<T> AggregatePipeline<T> {
  /// Create a new pipeline, optionally from a list of documents and options.
  ///
  /// For example, to create a pipeline for a Model without any other options:
  /// ```
  /// let pipeline = AggregationPipeline::new::<YourModel>(None, None);
  /// ```
  /// This is equivalent to calling `filter()` on the Model, which is preferred:
  /// ```
  /// let pipeline = YourModel::filter(None);
  /// ```
  pub fn new(
    pipeline: impl Into<Option<Vec<Document>>>,
    options: impl Into<Option<AggregateOptions>>,
  ) -> Self {
    AggregatePipeline(
      pipeline.into().unwrap_or(Vec::new()),
      options.into(),
      PhantomData,
    )
  }
  /// Add a single stage to the pipeline.
  pub fn add_stage(mut self, stage: Document) -> Self {
    self.0.extend(Some(stage));
    self
  }
  pub fn add_stages(mut self, stages: Vec<Document>) -> Self {
    self.0.extend(stages);
    self
  }
  pub fn with<C>(self) -> Self
  where
    T: Model + Relations<T, C>,
  {
    self.add_stages(T::with())
  }
  pub fn with_all(self) -> Self
  where
    T: RelationsAll,
  {
    self.add_stages(T::with_all())
  }
  /// Configures this pipeline with the specified options.
  pub fn config(mut self, options: AggregateOptions) -> Self {
    self.1 = Some(options);
    self
  }
  /// Returns a tuple containing aggreation stages and the aggregation options.
  pub fn finalize(self) -> (Vec<Document>, Option<AggregateOptions>) {
    (self.0, self.1)
  }
  /// Performs an aggregation using the specified aggregation pipeline and options, returing MongoDB documents.
  ///
  /// Use this over `aggregate()` when you don't need to process the data any further.
  pub async fn aggregate_raw(self, db: &Database) -> wither::Result<Vec<Document>>
  where
    T: Model,
  {
    let (pipeline, options) = self.finalize();
    Ok(aggregate!(db, pipeline, options, T))
  }
  /// Performs an aggregation using the specified aggregation pipeline and options, returing Models.
  pub async fn aggregate(self, db: &Database) -> wither::Result<Vec<T>>
  where
    T: Model,
  {
    let (pipeline, options) = self.finalize();
    Ok(aggregate!(db, pipeline, options, T, |e| async move {
      T::instance_from_document(e).ok()
    }))
  }
}

pub trait ToAggregatePipeline<T: Model> {
  fn filter(stages: impl Into<Option<Vec<Document>>>) -> AggregatePipeline<T> {
    AggregatePipeline(stages.into().unwrap_or(Vec::new()), None, PhantomData)
  }
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
impl<T: Model> ToAggregatePipeline<T> for T {}
