use sqlx::sqlite::SqlitePoolOptions;
use url::Url;
use urlnorm::UrlNormalizer;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let database_url = "sqlite://argus.db"; // Adjust the path to your database
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let normalizer = UrlNormalizer::default();

    // Update articles table
    let articles: Vec<(i64, String)> = sqlx::query_as("SELECT id, url FROM articles")
        .fetch_all(&pool)
        .await?;

    for (id, url) in articles {
        let parsed_url = match Url::parse(&url) {
            Ok(parsed) => parsed,
            Err(e) => {
                println!("Invalid URL in articles (id: {}): {} - {}", id, url, e);
                continue;
            }
        };

        let normalized_url = normalizer.compute_normalization_string(&parsed_url);

        sqlx::query("UPDATE articles SET normalized_url = ?1 WHERE id = ?2")
            .bind(&normalized_url)
            .bind(id)
            .execute(&pool)
            .await?;
    }

    // Update rss_queue table
    let queue_entries: Vec<(i64, String)> = sqlx::query_as("SELECT id, url FROM rss_queue")
        .fetch_all(&pool)
        .await?;

    for (id, url) in queue_entries {
        let parsed_url = match Url::parse(&url) {
            Ok(parsed) => parsed,
            Err(e) => {
                println!("Invalid URL in rss_queue (id: {}): {} - {}", id, url, e);
                continue;
            }
        };

        let normalized_url = normalizer.compute_normalization_string(&parsed_url);

        sqlx::query("UPDATE rss_queue SET normalized_url = ?1 WHERE id = ?2")
            .bind(&normalized_url)
            .bind(id)
            .execute(&pool)
            .await?;
    }

    Ok(())
}

