use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parse, parse_macro_input, DeriveInput, *};

/// Comments here!
#[proc_macro_derive(Orm, attributes(has_many, belongs_to, has_one, timestamps))]
pub fn relations(input: TokenStream) -> TokenStream {
  let ast = parse_macro_input!(input as DeriveInput);
  impl_relations(&ast).unwrap_or_else(|err| err.to_compile_error().into())
}

fn impl_relations(ast: &syn::DeriveInput) -> syn::Result<TokenStream> {
  let str_name = &ast.ident;
  let str_name_lowercase = format!("{}_id", str_name.to_string().to_lowercase());
  if let syn::Data::Struct(str) = &ast.data {
    if let syn::Fields::Named(named_fields) = &str.fields {
      let mut relations = quote! {};
      let mut with_all = quote! {};
      let mut save_rels = quote! {};
      for field in named_fields.named.iter() {
        let name = &field.ident;
        let ty: &syn::Type;

        let attr = field.attrs.iter().find(|e| {
          if let Some(segment) = e.path.segments.last() {
            return match segment.ident.to_string().as_str() {
              "has_many" | "belongs_to" | "has_one" => true,
              _ => false,
            };
          }
          false
        });
        if let (Some(name), Some(attr)) = (name, attr) {
          let name_quoted = format!("{}", name);
          let name_var = format!("${}", name);
          match attr
            .path
            .segments
            .last()
            .unwrap()
            .ident
            .to_string()
            .as_str()
          {
            "has_many" => {
              require_serde_omit_none(field, "has_many")?;
              let err = syn::Error::new(
                name.span(),
                format!("has_many requires an Option<Vec<Model>> field"),
              );
              let unwrapped = SingleParamType::from(&field.ty).ok_or(err.clone())?;
              if unwrapped.0.eq("Option") {
                let unwrapped = SingleParamType::from(unwrapped.1).ok_or(err.clone())?;
                if unwrapped.0.eq("Vec") {
                  if let None = SingleParamType::from(unwrapped.1) {
                    ty = unwrapped.1;
                  } else {
                    return Err(err);
                  }
                } else {
                  return Err(err);
                }
              } else {
                return Err(err);
              }
              relations.extend(quote! {
                #[async_trait]
                impl Relations<Self, Vec<#ty>> for #str_name {
                  fn _with() -> Vec<Document> {
                    vec![doc! {
                      "$lookup": {
                        "from": #ty::COLLECTION_NAME,
                        "localField": "_id",
                        "foreignField": #str_name_lowercase,
                        "as": #name_quoted
                      }
                    }]
                  }
                  async fn save_rel(
                    &self,
                    db: &Database,
                    other: &Vec<#ty>,
                    options: Option<wither::mongodb::options::UpdateOptions>,
                  ) -> wither::Result<wither::mongodb::results::UpdateResult> {
                    let pid = self
                      .id()
                      .ok_or(wither::WitherError::ModelIdRequiredForOperation)?;
                    let options = options.unwrap_or(
                      wither::mongodb::options::UpdateOptions::builder()
                        .upsert(true)
                        .build(),
                    );
                    let cids = other.iter().filter_map(|e| e.id()).collect::<Vec<_>>();
                    Ok(
                      #ty::collection(db)
                        .update_many(
                          doc! { "_id": { "$in": cids } },
                          doc! { "$set": { #str_name_lowercase: pid } },
                          options,
                        )
                        .await?,
                    )
                  }
                }
              });
              with_all.extend(quote! {
                docs.extend(Self::with::<Vec<#ty>>());
              });
              save_rels.extend(quote! {
                if let Some(rel) = &self.#name {
                  self.save_rel(db, rel, options.clone()).await?;
                }
              });
            }
            "belongs_to" => {
              require_serde_omit_none(field, "belongs_to")?;
              let err = syn::Error::new(name.span(), "belongs_to requires an Option<Model> field");
              let args: BelongsToArgs = attr.parse_args().unwrap_or_default();
              let unwrapped = SingleParamType::from(&field.ty).ok_or(err.clone())?;
              if unwrapped.0.eq("Option") {
                if let None = SingleParamType::from(unwrapped.1) {
                  ty = unwrapped.1;
                } else {
                  return Err(err);
                }
              } else {
                return Err(err);
              }
              let name_id = format!("{}_id", args.local_field.unwrap_or(name.clone()));
              relations.extend(quote! {
                #[async_trait]
                impl Relations<Self, #ty> for #str_name {
                  fn _with() -> Vec<Document> {
                    vec![
                      doc! {
                        "$lookup": {
                          "from": #ty::COLLECTION_NAME,
                          "localField": #name_id,
                          "foreignField": "_id",
                          "as": #name_quoted
                        }
                      },
                      doc! { "$unwind": #name_var },
                    ]
                  }

                  async fn save_rel(
                    &self,
                    db: &Database,
                    other: &#ty,
                    options: Option<wither::mongodb::options::UpdateOptions>,
                  ) -> wither::Result<wither::mongodb::results::UpdateResult> {
                    let sid = self
                      .id()
                      .ok_or(wither::WitherError::ModelIdRequiredForOperation)?;
                    let oid = other
                      .id()
                      .ok_or(wither::WitherError::ModelIdRequiredForOperation)?;
                    Ok(
                      Self::collection(db)
                        .update_one(
                          doc! { "_id": sid },
                          doc! { "$set": { #name_id: oid } },
                          options,
                        )
                        .await?,
                    )
                  }
                }
              });
              with_all.extend(quote! {
                docs.extend(Self::with::<#ty>());
              });
              save_rels.extend(quote! {
                if let Some(rel) = &self.director {
                  self.save_rel(db, rel, options.clone()).await?;
                }
              });
            }
            "has_one" => {
              require_serde_omit_none(field, "has_one")?;
              let err = syn::Error::new(
                name.span(),
                format!("has_one requires an Option<Model> field. If this creates an infinite struct, remove the belongs_to relation from the child struct."),
              );
              let unwrapped = SingleParamType::from(&field.ty).ok_or(err.clone())?;
              if unwrapped.0.eq("Option") {
                if let None = SingleParamType::from(unwrapped.1) {
                  ty = unwrapped.1;
                } else {
                  return Err(err);
                }
              } else {
                return Err(err);
              }
              relations.extend(quote! {
                #[async_trait]
                impl Relations<Self, #ty> for #str_name {
                  fn _with() -> Vec<Document> {
                    vec![
                      doc! {
                        "$lookup": {
                          "from": #ty::COLLECTION_NAME,
                          "localField": "_id",
                          "foreignField": #str_name_lowercase,
                          "as": #name_quoted
                        }
                      },
                      doc! { "$unwind": #name_var },
                    ]
                  }

                  async fn save_rel(
                    &self,
                    db: &Database,
                    other: &#ty,
                    options: Option<wither::mongodb::options::UpdateOptions>,
                  ) -> wither::Result<wither::mongodb::results::UpdateResult> {
                    let sid = self
                      .id()
                      .ok_or(wither::WitherError::ModelIdRequiredForOperation)?;
                    let oid = other
                      .id()
                      .ok_or(wither::WitherError::ModelIdRequiredForOperation)?;
                    Ok(
                      #ty::collection(db)
                        .update_one(
                          doc! { "_id": oid },
                          doc! { "$set": { #str_name_lowercase: sid } },
                          options,
                        )
                        .await?,
                    )
                  }
                }
              });
              with_all.extend(quote! {
                docs.extend(Self::with::<#ty>());
              });
              save_rels.extend(quote! {
                if let Some(rel) = &self.#name {
                  self.save_rel(db, rel, options.clone()).await?;
                }
              });
            }
            _ => {}
          }
        }
      }
      let finaldoc = quote! {
        #relations
        #[async_trait]
        impl RelationsAll for #str_name {
          fn with_all() -> Vec<Document> {
            let mut docs = Vec::new();
            #with_all
            docs
          }
          async fn save_rels(&self, db: &Database, options: Option<wither::mongodb::options::UpdateOptions>) -> wither::Result<()> {
            #save_rels
            Ok(())
          }
        }
      };
      return Ok(finaldoc.into());
    }
  }
  Err(syn::Error::new(
    ast.ident.span(),
    "Relationships can only be derived for named structs",
  ))
}

/// Unwraps a single A<B> into B, extracting A as an Ident.
struct SingleParamType<'a>(&'a Ident, &'a syn::Type);

impl<'a> SingleParamType<'a> {
  fn from(value: &'a syn::Type) -> Option<SingleParamType<'a>> {
    if let syn::Type::Path(syn::TypePath { qself: None, path }) = value {
      if let Some(segment) = path.segments.last() {
        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
          if args.args.len() == 1 {
            let gen_arg = args.args.first().unwrap();
            if let syn::GenericArgument::Type(ty) = gen_arg {
              return Some(SingleParamType(&segment.ident, ty));
            }
          }
        }
      }
    }
    None
  }
}

fn require_serde_omit_none(field: &Field, rel_type: &str) -> syn::Result<()> {
  if let Some(result) = field.attrs.iter().find(|e| e.path.is_ident("serde")) {
    result.parse_args::<SerdeOmitNone>().map(|_| ())
  } else {
    Err(syn::Error::new(
      (&field.ident).clone().unwrap().span(),
      format!(
        "{} requires the attribute #[serde(skip_serializing_if = \"Option::is_none\")",
        rel_type
      ),
    ))
  }
}

struct SerdeOmitNone;
impl Parse for SerdeOmitNone {
  fn parse(input: parse::ParseStream) -> Result<Self> {
    let punct = punctuated::Punctuated::<Expr, Token![,]>::parse_terminated(input)?;
    let attr = punct.iter().find(|e| {
      if let Expr::Assign(expr) = e {
        let left = expr.left.as_ref();
        let right = expr.right.as_ref();
        if let (Expr::Path(path), Expr::Lit(lit)) = (left, right) {
          let key = path.path.get_ident();
          let value = &lit.lit;
          if let (Some(key), Lit::Str(value)) = (key, value) {
            if key.eq("skip_serializing_if") && value.value() == "Option::is_none" {
              return true;
            } else {
              return false;
            }
          }
        }
      }
      false
    });
    if let Some(_) = attr {
      Ok(SerdeOmitNone)
    } else {
      Err(input.error("This relationship requires the attribute #[serde(skip_serializing_if = \"Option::is_none\")]"))
    }
  }
}

#[derive(Default)]
struct BelongsToArgs {
  local_field: Option<Ident>,
}

impl Parse for BelongsToArgs {
  fn parse(input: parse::ParseStream) -> Result<Self> {
    if input.is_empty() {
      return Ok(BelongsToArgs { local_field: None });
    }
    let mut local_field: Option<Ident> = None;
    while !input.is_empty() {
      let key: Ident = input.parse()?;
      let keydata = key.clone();
      let keydata = keydata.to_string();
      let _: Token!(=) = input.parse()?;
      if input.peek(Ident) && keydata == "local_field" {
        local_field = Some(input.parse()?);
      }
      if !input.is_empty() {
        let _: Token!(,) = input.parse()?;
      }
    }
    Ok(BelongsToArgs { local_field })
  }
}
