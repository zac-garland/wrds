# WRDS Chromote Starter Script
# A simple template for scraping WRDS subscriptions

library(chromote)
library(dplyr)

# Initialize browser
cat("Starting browser...\n")
b <- ChromoteSession$new()
b$view()
# Navigate to WRDS login
cat("Navigating to WRDS...\n")
b$Page$navigate("https://wrds-www.wharton.upenn.edu/login/")
Sys.sleep(3)

# Wait for manual authentication
cat("Please log in to WRDS in the browser window...\n")
cat("Press Enter in R console after you've successfully logged in: ")
readline()

# Navigate to data library (try common paths)
cat("Navigating to data library...\n")
b$Page$navigate("https://wrds-www.wharton.upenn.edu/data/")
Sys.sleep(3)

# Alternative navigation if direct URL doesn't work
# b$Runtime$evaluate("
#   const dataLink = Array.from(document.links).find(link =>
#     link.textContent.toLowerCase().includes('data') ||
#     link.href.includes('/data/')
#   );
#   if (dataLink) dataLink.click();
# ")

# Extract subscription information
cat("Extracting datasets...\n")
datasets <- b$Runtime$evaluate("
  // Look for dataset elements using common selectors
  const datasets = [];

  // Try cards/tiles first
  document.querySelectorAll('.card, .tile, .dataset-item').forEach(card => {
    const title = card.querySelector('h1, h2, h3, h4, .title')?.textContent?.trim();
    const desc = card.querySelector('p, .description')?.textContent?.trim();
    if (title) datasets.push({name: title, description: desc || ''});
  });

  // Try table rows if no cards found
  if (datasets.length === 0) {
    document.querySelectorAll('tr').forEach(row => {
      const cells = row.querySelectorAll('td');
      if (cells.length >= 2) {
        const name = cells[0].textContent.trim();
        const desc = cells[1].textContent.trim();
        if (name && name.length > 2) datasets.push({name, description: desc});
      }
    });
  }

  // Try links as fallback
  if (datasets.length === 0) {
    document.querySelectorAll('a[href*=\"data\"]').forEach(link => {
      const name = link.textContent.trim();
      if (name && name.length > 2) {
        datasets.push({name, description: ''});
      }
    });
  }

  return datasets;
")

# Convert to data frame
if (!is.null(datasets$result$value) && length(datasets$result$value) > 0) {
  df <- do.call(rbind, lapply(datasets$result$value, function(x) {
    data.frame(
      dataset = x$name %||% "Unknown",
      description = x$description %||% "",
      stringsAsFactors = FALSE
    )
  }))

  cat("Found", nrow(df), "datasets:\n")
  print(df)

} else {
  cat("No datasets found automatically. Checking page content...\n")

  # Get page info for debugging
  page_info <- b$Runtime$evaluate("({
    title: document.title,
    url: window.location.href,
    content: document.body.textContent.substring(0, 200)
  })")

  cat("Current page:", page_info$result$value$title, "\n")
  cat("URL:", page_info$result$value$url, "\n")
  cat("Content preview:", page_info$result$value$content, "\n")
}

# Take screenshot for reference
cat("Taking screenshot...\n")
screenshot <- b$Page$captureScreenshot()
writeBin(base64enc::base64decode(screenshot$data), "wrds_page.png")

# Manual inspection helper
cat("\nTo manually inspect elements, use:\n")
cat("b$Runtime$evaluate('document.querySelector(\"YOUR_SELECTOR\").textContent')\n")
cat("Common selectors to try: '.card', 'table tr', '.subscription', 'a[href*=\"data\"]'\n")

# Close browser when done
cat("Run b$close() when finished\n")

# Example of additional commands you can run:
#
# # Search for specific dataset
# b$Runtime$evaluate("
#   Array.from(document.querySelectorAll('*')).find(el =>
#     el.textContent.toLowerCase().includes('compustat')
#   )?.textContent
# ")
#
# # Get all links
# b$Runtime$evaluate("
#   Array.from(document.links).map(link => ({
#     text: link.textContent.trim(),
#     href: link.href
#   })).slice(0, 10)
# ")
#
# # Navigate to specific section
# b$Runtime$evaluate("document.querySelector('a[href*=\"compustat\"]')?.click()")
#
# b$close()  # Close when done
