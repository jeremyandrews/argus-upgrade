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

    // Add 'normalized_url' column to the 'articles' table
    sqlx::query("ALTER TABLE articles ADD COLUMN normalized_url TEXT;")
        .execute(&pool)
        .await?;

    // Add 'normalized_url' column to the 'rss_queue' table
    sqlx::query("ALTER TABLE rss_queue ADD COLUMN normalized_url TEXT;")
        .execute(&pool)
        .await?;

    // Create a unique index on 'articles.normalized_url'
    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_normalized_url ON articles(normalized_url);")
        .execute(&pool)
        .await?;

    // Create a unique index on 'rss_queue.normalized_url'
    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_rss_queue_normalized_url ON rss_queue(normalized_url);")
        .execute(&pool)
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
