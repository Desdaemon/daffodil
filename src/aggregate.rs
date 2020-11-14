use std::marker::PhantomData;

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
/// let products = T::filter(doc! { "name": "Computer" })
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
  /// It is recommended to use `add_stages()` over this whenever possible.
  pub fn add_stage(mut self, stage: Document) -> Self {
    self.0.extend_one(stage);
    self
  }
  pub fn add_stages(mut self, stages: Vec<Document>) -> Self {
    self.0.extend(stages);
    self
  }
  pub fn with<C: Model>(self) -> Self
  where
    T: Relations<T, C>,
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
