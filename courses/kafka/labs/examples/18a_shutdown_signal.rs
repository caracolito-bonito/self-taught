use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Waiting for Ctrl+C");
    tokio::signal::ctrl_c().await?;
    println!("\nSignal is received");
    Ok(())
}
