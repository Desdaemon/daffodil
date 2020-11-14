#![feature(extend_one)]

pub mod aggregate;
pub mod orm;
pub mod prelude;
pub use wither;

#[cfg(test)]
mod tests {

  use crate::{aggregate, prelude::*};

  #[derive(Debug, Orm, Serialize, Deserialize, Default)]
  pub(crate) struct Person {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    #[has_many]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub movies: Option<Vec<Movie>>,

    pub name: String,
    pub achievements: Vec<String>,
  }

  impl Model for Person {
    const COLLECTION_NAME: &'static str = "people";

    fn id(&self) -> Option<ObjectId> {
      self.id.clone()
    }

    fn set_id(&mut self, id: ObjectId) {
      self.id = Some(id);
    }
  }

  #[derive(Debug, Model, Orm, Serialize, Deserialize, Default)]
  pub(crate) struct Movie {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    #[has_many]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staff: Option<Vec<Person>>,

    #[belongs_to]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub director: Option<Person>,

    #[has_one]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setting: Option<Setting>,

    pub name: String,
    pub rating: i32,
  }

  #[derive(Debug, Model, Serialize, Deserialize, Default)]
  pub(crate) struct Setting {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    pub name: String,
  }
  #[actix_rt::test]
  async fn test_relations() -> wither::Result<()> {
    let client = wither::mongodb::Client::with_uri_str("mongodb://localhost:27017").await?;
    let db = client.database("daffodil_test");
    db.drop(None).await?;

    // Create
    [
      "Doraemon", "Nobita", "Jaian", "Dekisugi", "Shizuka", "Tsuneo",
    ]
    .iter()
    .map(|name| Person {
      name: String::from(*name),
      ..Person::default()
    })
    .collect::<Vec<_>>()
    .save(&db, None)
    .await?;

    (0..=9)
      .map(|e: u16| Movie {
        name: format!("Movie {}", e),
        ..Movie::default()
      })
      .collect::<Vec<_>>()
      .save(&db, None)
      .await?;

    // Update
    {
      let staff = aggregate!(
        &db,
        pipeline: vec![doc! { "$match": { "name": { "$in": ["Nobita", "Doraemon"] } } }],
        Person,
        |e| async move { Person::instance_from_document(e).ok() }
      );

      let director = Person::find_one(&db, doc! { "name": "Dekisugi" }, None).await?;

      let movie = Movie::find_one(&db, doc! { "name": "Movie 0" }, None)
        .await?
        .expect("Failed to get Movie named 'Movie 0' from the database");
      let mut setting = Setting {
        name: "Prehistoric".into(),
        ..Setting::default()
      };
      setting.save(&db, None).await?;

      let mut from_web = Movie {
        name: "Bad Movie Name".into(),
        rating: -100,
        ..Movie::default()
      };
      from_web.set_id(movie.id.unwrap());
      from_web.save(&db, None).await?;

      from_web.staff = Some(staff);
      from_web.setting = Some(setting);
      from_web.director = director;
      from_web.save_rels(&db, None).await?;
      let movies = aggregate!(&db, pipeline: Movie::with_all(), Movie, |e| async move {
        Movie::instance_from_document(e).ok()
      });
      println!(
        "{:#?}",
        movies
          .first()
          .expect("Failed to retrieve Movies from aggregation")
      );
    }

    Ok(())
  }
}
