pub mod aggregate;
pub mod orm;
pub mod prelude;
pub(crate) mod private;
pub use daffodil_derive;
pub use wither;

#[cfg(test)]
mod tests {
  use crate::orm::Timestamps;
  use crate::prelude::*;
  use wither::bson::Bson;

  /// relation|field|type
  /// -|-|-|
  /// has_many|movies|`Vec<Movie>`
  #[derive(Debug, Model, Orm, Serialize, Deserialize, Default, Clone)]
  #[model(collection_name = "people")]
  pub(crate) struct Person {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    #[has_many]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub movies: Option<Vec<Movie>>,

    pub name: String,
    pub achievements: Vec<String>,

    #[timestamps(once)]
    created_at: Bson,

    #[timestamps]
    updated_at: Bson,
  }

  #[derive(Debug, Model, Orm, Serialize, Deserialize, Default, Clone)]
  pub(crate) struct Movie {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    pub name: String,
    pub rating: i32,

    #[has_many]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staff: Option<Vec<Person>>,

    #[belongs_to]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub director: Option<Person>,

    #[has_one]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setting: Option<Setting>,

    #[timestamps(once)]
    created_at: Bson,

    #[timestamps]
    updated_at: Bson,
  }

  #[derive(Debug, Orm, Model, Serialize, Deserialize, Default, Clone)]
  pub(crate) struct Setting {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    pub name: String,

    #[timestamps(once)]
    pub created_at: Bson,
    #[timestamps]
    pub updated_at: Bson,
  }

  async fn setup_db() -> wither::Result<Database> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let client = wither::mongodb::Client::with_uri_str("mongodb://localhost:27017").await?;
    let db = client.database("daffodil_test");
    db.drop(None).await?;
    Ok(db)
  }

  async fn test_save_rel(db: &Database) -> wither::Result<()> {
    let movie = Movie {
      name: "Titanic".into(),
      ..Movie::default()
    }
    .persist_with(db)
    .await?;

    let mut staff = ["Titanic Staff 1", "Titanic Staff 2"]
      .iter()
      .map(|name| Person {
        name: String::from(*name),
        ..Person::default()
      })
      .collect::<Vec<_>>();
    staff.save(db, None).await?;

    movie.save_rel(db, &Some(staff), None).await?;
    let movie = Movie::filter(vec![doc! { "$match": {"_id": movie.id().unwrap()}}])
      .with::<Vec<Person>>()
      .aggregate(db)
      .await?;
    println!("test_save_rel results: {:#?}", movie);

    Ok(())
  }
  async fn test_save_rels(db: &Database) -> wither::Result<()> {
    let director = Person {
      name: "Titanic Director".into(),
      ..Person::default()
    }
    .persist_with(db)
    .await?;

    let setting = Setting {
      name: "Atlantic Ocean".into(),
      ..Setting::default()
    }
    .persist(db)
    .await?;

    let mut movie = Movie::filter(vec![doc! {"$match": {"name": "Titanic"}}])
      .with::<Vec<_>>()
      .aggregate(db)
      .await?
      .first()
      .cloned()
      .expect("Expected to retrieve exactly one movie named 'Titanic'");

    movie.director = Some(director);
    movie.setting = Some(setting);
    movie.name = String::from("Groundhog Day");
    movie.rating = 100;
    movie.save_rels(db, None, None).await?;
    println!("test_save_rels results: {:#?}", movie);

    Ok(())
  }
  async fn test_delete_rel(db: &Database) -> wither::Result<()> {
    let movie = Movie::filter(vec![doc! {"$match": {"name": "Titanic"}}])
      .with_all()
      .aggregate(db)
      .await?
      .first()
      .cloned()
      .expect("Expected to retrieve exactly one movie named 'Titanic'");
    movie.delete_rel::<Setting>(db).await?;

    assert_eq!(
      0,
      Setting::collection(db).count_documents(None, None).await?
    );

    Ok(())
  }
  async fn test_delete_rels(db: &Database) -> wither::Result<()> {
    Ok(())
  }
  #[actix_rt::test]
  async fn test_relations() -> wither::Result<()> {
    let db = setup_db().await?;
    test_save_rel(&db).await.expect("test_save_rel failed");
    test_save_rels(&db).await.expect("test_save_rels failed");
    test_delete_rel(&db).await.expect("test_delete_rel failed");
    test_delete_rels(&db)
      .await
      .expect("test_delete_rels failed");
    Ok(())
  }
}
