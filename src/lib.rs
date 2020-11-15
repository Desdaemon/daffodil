#![feature(extend_one)]

pub mod aggregate;
pub mod orm;
pub mod prelude;
pub use daffodil_derive;
pub use wither;

#[cfg(test)]
mod tests {

  use wither::bson::Bson;

  use crate::orm::Timestamps;
  use crate::prelude::*;

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
    created_at: Bson,
    updated_at: Bson,
  }

  impl Timestamps for Person {
    fn timestamps() -> Bson {
      Bson::Document(doc! { "updated_at": true })
    }

    fn timestamps_new() -> Bson {
      Bson::Document(doc! { "created_at": true, "updated_at": true })
    }
  }

  #[derive(Debug, Model, Orm, Serialize, Deserialize, Default, Clone)]
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

    created_at: Bson,
    updated_at: Bson,
  }

  impl Timestamps for Movie {
    fn timestamps() -> Bson {
      Bson::Document(doc! { "updated_at": true })
    }

    fn timestamps_new() -> Bson {
      Bson::Document(doc! { "updated_at": true, "created_at": true })
    }
  }

  #[derive(Debug, Model, Serialize, Deserialize, Default, Clone)]
  pub(crate) struct Setting {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,

    pub name: String,

    pub created_at: Bson,
    pub updated_at: Bson,
  }

  impl Timestamps for Setting {
    fn timestamps() -> Bson {
      Bson::Document(doc! { "updated_at": true })
    }

    fn timestamps_new() -> Bson {
      Bson::Document(doc! { "updated_at": true, "created_at": true })
    }
  }

  #[actix_rt::test]
  async fn test_relations() -> wither::Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let client = wither::mongodb::Client::with_uri_str("mongodb://localhost:27017").await?;
    let db = client.database("daffodil_test");
    db.drop(None).await?;

    // Create
    let mut people = [
      "Doraemon", "Nobita", "Jaian", "Dekisugi", "Shizuka", "Tsuneo",
    ]
    .iter()
    .map(|name| Person {
      name: String::from(*name),
      ..Person::default()
    })
    .collect::<Vec<_>>();
    people.save(&db, None).await?;

    let mut movies = (0..=9)
      .map(|e: u16| Movie {
        name: format!("Movie {}", e),
        ..Movie::default()
      })
      .collect::<Vec<_>>();
    movies.save(&db, None).await?;

    // Update
    {
      let staff = people
        .iter()
        .filter(|e| e.name == "Nobita" || e.name == "Doraemon")
        .map(|e| e.clone())
        .collect::<Vec<_>>();

      let director = people.into_iter().find(|e| e.name == "Dekisugi");

      let movie = movies
        .into_iter()
        .find(|e| e.name == "Movie 0")
        .expect("Failed to get movie named 'Movie 0' from local");

      let mut setting = Setting {
        name: "Prehistoric".into(),
        ..Setting::default()
      };
      setting.save(&db, None).await?;
      let setting = setting
        .update(
          &db,
          None,
          doc! { "$currentDate": Setting::timestamps_new() },
          None,
        )
        .await?;

      let from_web = Movie {
        name: "Bad Movie Name".into(),
        rating: -100,
        id: movie.id,
        ..Movie::default()
      };
      let mut from_web = from_web
        .update(
          &db,
          None,
          doc! { "$currentDate" : Movie::timestamps() },
          None,
        )
        .await?;

      from_web.save_rel(&db, &staff, None).await?;

      from_web.setting = Some(setting);
      from_web.director = director;
      from_web.save_rels(&db, None).await?;
      let movies = Movie::filter(None)
        .with::<Vec<Person>>()
        .with::<Setting>()
        .aggregate(&db)
        .await?;
      println!("{}", serde_json::to_string_pretty(&movies).unwrap());
    }

    Ok(())
  }
}
