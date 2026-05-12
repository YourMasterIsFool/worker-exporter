use std::result;

use anyhow::{Ok, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, QueryBuilder};

use crate::core::traits::extractor::build_query_trait::BuildQueryTrait;

use super::model::CandidatePoolSnapshotModel;

pub struct CandidatePoolQuerySnapshot {
    pool: PgPool,
}

// impl BuildQueryTrait for CandidatePoolQuerySnapshot {
//     fn query(&self) -> &'static str {
//         r#"
// 	    SELECT name, nik, id "#
//     }
//     fn table(&self) -> &'static str {
//         "candidate_pool"
//     }
// }

// #[async_trait::async_trait]
// impl IncrementalExtract for CandidatePoolQuerySnapshot {
//     async fn extract(
//         &self,
//         cursor: Option<String>,
//         limit: u64,
//     ) -> Result<(Vec<serde_json::Value>, Option<String>)> {
//         let sql = format!("{} from {} where 1=1", self.query(), self.table());
//         let last_cursor:Option<String> = None

//         let mut qb = QueryBuilder::new(sql);

//         if let Some(c) = cursor {
//             qb.push("id > ").push_bind(c);
//         }

//         qb.push(" LIMIT  ").push_bind(limit);

//         let result:Vec<CandidatePoolSnapshotModel> = qb
//             .build_query_as::<CandidatePoolSnapshotModel>()
//             .fetch_all(&self.pool)
//             .await?;

//         let mapping =  result.into_iter().map(|f| f.).collect();
//         if let Some(last) = result.last() {
//             last_cursor = last.id
//         }
//         Ok((result, last_cursor))
//     }
// }
// // #[async_trait::async_trait]
// // impl IncrementalSource for CandidatePoolQuerySnapshot {
// //     type Watermark = i64;
// //     type Output = CandidatePoolSnapshotModel;
// //
// //     async fn fetch_since(
// //         &self,
// //         watermark: Option<Self::Watermark>,
// //         limit: i64,
// //     ) -> Result<Vec<Self::Output>> {
// //         let sql = format!("{} {} where 1=1 ", self.query(), self.table());
// //
// //         let mut qb = QueryBuilder::new(sql);
// //
// //         qb.push(" id > ");
// //         qb.push_bind(watermark);
// //
// //         qb.push("limit ");
// //         qb.push_bind(limit);
// //         let query = qb
// //             .build_query_as::<CandidatePoolSnapshotModel>()
// //             .fetch_all(&self.pool)
// //             .await?;
// //
// //         Ok(query)
// //     }
// // }
