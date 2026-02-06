# Script to regenerate test data with proper FK relationships
param(
    [string]$DataDir = "..\import_testdata_all_in_one"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Read valid user IDs from users CSV (skip invalid ones without proper UUID)
Write-Host "Reading user IDs from CSV..."
$userIds = @()
Get-Content "$DataDir\users_huge.csv" | Select-Object -Skip 1 | ForEach-Object {
    $cols = $_ -split ","
    $id = $cols[0]
    # Check if it's a valid UUID format (36 chars with dashes)
    if ($id -match "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$") {
        $userIds += $id
    }
}
Write-Host "Found $($userIds.Count) valid user IDs"

# Generate articles with valid author_ids
Write-Host "Generating articles_huge.ndjson with valid author_ids..."
$articleIds = @()
$articles = @()
$statuses = @("draft", "published")
$tagOptions = @(
    '["intro"]',
    '["security"]', 
    '["performance", "testing"]',
    '["validation", "intro", "testing", "welcome"]',
    '["api", "design"]',
    '[]'
)

$random = New-Object System.Random

for ($i = 0; $i -lt 15000; $i++) {
    $articleId = [guid]::NewGuid().ToString()
    $authorId = $userIds[$random.Next($userIds.Count)]
    $status = $statuses[$random.Next($statuses.Count)]
    $tags = $tagOptions[$random.Next($tagOptions.Count)]
    
    $slug = "article-slug-$i"
    $title = "Article Title $i"
    $body = "This is the body content for article $i. " * 5
    
    $publishedAt = ""
    if ($status -eq "published") {
        $publishedAt = ', "published_at": "2024-01-01T00:00:00Z"'
    }
    
    # Introduce some intentional errors (about 3%)
    $hasError = $random.Next(100) -lt 3
    
    if ($hasError) {
        $errorType = $random.Next(4)
        switch ($errorType) {
            0 { $slug = "" }  # Empty slug
            1 { $authorId = "invalid-uuid" }  # Invalid author
            2 { $status = "invalid_status" }  # Invalid status
            3 { 
                $status = "draft"
                $publishedAt = ', "published_at": "2024-01-01T00:00:00Z"'  # Draft with published_at
            }
        }
    }
    
    $json = "{`"id`": `"$articleId`", `"slug`": `"$slug`", `"title`": `"$title`", `"body`": `"$body`", `"author_id`": `"$authorId`", `"tags`": $tags, `"status`": `"$status`"$publishedAt}"
    $articles += $json
    
    if (-not $hasError) {
        $articleIds += $articleId
    }
}

$articles | Out-File "$DataDir\articles_huge.ndjson" -Encoding utf8
Write-Host "Generated $($articles.Count) articles, $($articleIds.Count) expected valid"

# Generate comments with valid article_ids and user_ids
Write-Host "Generating comments_huge.ndjson with valid FKs..."
$comments = @()

for ($i = 0; $i -lt 20000; $i++) {
    $commentId = [guid]::NewGuid().ToString()
    $articleId = $articleIds[$random.Next($articleIds.Count)]
    $userId = $userIds[$random.Next($userIds.Count)]
    $body = "This is comment $i on the article. " * 3
    $createdAt = "2024-01-01T00:00:00Z"
    
    # Introduce some intentional errors (about 3%)
    $hasError = $random.Next(100) -lt 3
    
    if ($hasError) {
        $errorType = $random.Next(4)
        switch ($errorType) {
            0 { $body = "" }  # Empty body
            1 { $articleId = "invalid-article-id" }  # Invalid article FK
            2 { $userId = "invalid-user-id" }  # Invalid user FK
            3 { $body = "x" * 11000 }  # Body too long
        }
    }
    
    $json = "{`"id`": `"$commentId`", `"article_id`": `"$articleId`", `"user_id`": `"$userId`", `"body`": `"$body`", `"created_at`": `"$createdAt`"}"
    $comments += $json
}

$comments | Out-File "$DataDir\comments_huge.ndjson" -Encoding utf8
Write-Host "Generated $($comments.Count) comments"

Write-Host "`nTest data regenerated with proper FK relationships!"
