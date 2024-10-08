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
    println!("Creating unique index on articles...");
    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_normalized_url ON articles(normalized_url);")
        .execute(&pool)
        .await?;

    // Create a unique index on 'rss_queue.normalized_url'
    println!("Creating unique index on rss_queue...");
    sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_rss_queue_normalized_url ON rss_queue(normalized_url);")
        .execute(&pool)
        .await?;

    let normalizer = UrlNormalizer::default();

    // Update articles table
    let articles: Vec<(i64, String)> = sqlx::query_as("SELECT id, url FROM articles")
        .fetch_all(&pool)
        .await?;

    for (i, (id, url)) in articles.iter().enumerate() {
        let parsed_url = match Url::parse(&url) {
            Ok(parsed) => parsed,
            Err(e) => {
                println!("Invalid URL in articles (id: {}): {} - {}", id, url, e);
                continue;
            }
        };

        let normalized_url = normalizer.compute_normalization_string(&parsed_url);

        // Check if the normalized_url already exists in the 'articles' table
        let existing_id: Option<i64> =
            sqlx::query_scalar("SELECT id FROM articles WHERE normalized_url = ?1 LIMIT 1")
                .bind(&normalized_url)
                .fetch_optional(&pool)
                .await?;

        // If normalized_url already exists and it's a different record, skip this one
        if let Some(existing_id) = existing_id {
            if existing_id != *id {
                println!(
            "Skipping record with id {}: normalized_url '{}' already exists in record with id {}",
            id, normalized_url, existing_id
        );
                continue; // Skip updating this record
            }
        }

        sqlx::query("UPDATE articles SET normalized_url = ?1 WHERE id = ?2")
            .bind(&normalized_url)
            .bind(id)
            .execute(&pool)
            .await?;

        if i % 500 == 0 {
            println!("Processed {} articles...", i);
        }
    }

    // Update rss_queue table
    let queue_entries: Vec<(i64, String)> = sqlx::query_as("SELECT id, url FROM rss_queue")
        .fetch_all(&pool)
        .await?;

    for (i, (id, url)) in queue_entries.iter().enumerate() {
        let parsed_url = match Url::parse(&url) {
            Ok(parsed) => parsed,
            Err(e) => {
                println!("Invalid URL in rss_queue (id: {}): {} - {}", id, url, e);
                continue;
            }
        };

        let normalized_url = normalizer.compute_normalization_string(&parsed_url);

        // Check if the normalized_url already exists in the 'rss_queue' table
        let existing_id: Option<i64> =
            sqlx::query_scalar("SELECT id FROM rss_queue WHERE normalized_url = ?1 LIMIT 1")
                .bind(&normalized_url)
                .fetch_optional(&pool)
                .await?;

        // If normalized_url already exists and it's a different record, skip this one
        if let Some(existing_id) = existing_id {
            if existing_id != *id {
                println!(
            "Skipping record with id {}: normalized_url '{}' already exists in record with id {}",
            id, normalized_url, existing_id
        );
                continue; // Skip updating this record
            }
        }

        sqlx::query("UPDATE rss_queue SET normalized_url = ?1 WHERE id = ?2")
            .bind(&normalized_url)
            .bind(id)
            .execute(&pool)
            .await?;
        if i % 500 == 0 {
            println!("Processed {} queue entries...", i);
        }
    }

    Ok(())
}
