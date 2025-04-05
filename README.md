# Why Whelm?

<div style="font-size: 0.95em;"><p>
Just for a second assume you're a content creator on YouTube (I know most of you are). Don't you just think, there should be a tool which should give me each and every detail about my new videos like how audience perceived it, what they want, feedbacks from them, the overall sentiment in audience so that I can better my content?</p>

Imagine uploading a video that you spent days creating - scripting, filming, editing - only to face the daunting task of manually sifting through hundreds or thousands of comments to understand what worked and what didn't. You're left wondering: Did my audience actually like this content? What specific aspects resonated with them? What should I change for my next video?

**Content creation shouldn't be guesswork**. You shouldn't have to rely on basic metrics like views and likes to determine if your content strategy is working. What if you could have a personal analyst that processes every comment, extracts meaningful insights, and delivers clear recommendations directly to you?

**That's exactly what Whelm does!** ***Whelm*** is an intelligent analytics system designed to help content creators understand audience perception and improve their content strategy. By automating the collection and analysis of YouTube comments. Processing comments from videos published within the last week to offer timely feedback.
</div>

## How Whelm Works?

Whelm works like your dedicated research team, constantly monitoring your YouTube presence. Every day, it collects fresh comments from your recently published videos. These comments pass through intelligent processing that understands language nuances beyond simple keywords.

<div  align="left">
  <img src="https://ik.imagekit.io/fcaqoy5tdf/workflow.jpg?updatedAt=1743622019107" alt="Placeholder Image" width="98%" style="border-radius: 10px;">
</div>

Whelm reads between the lines to identify sentiment, extracting how viewers truly feel about your content. The system then transforms this raw feedback into clear insights and actionable recommendations. All this happens automatically in the background while you focus on creating your next masterpiece.

## Behind the Scenes

Whelm operates through a sophisticated six-stage pipeline that turns viewer comments into creator gold. 

- The journey begins with fetching recent comments from your videos using YouTube's API. 
- These comments are stored securely before undergoing preprocessing to clean and prepare them for analysis. 
- The RoBERTa model then evaluates each comment's emotional tone, categorizing them as positive, negative, or neutral.

<div  align="left">
  <img src="https://ik.imagekit.io/fcaqoy5tdf/structure.jpg?updatedAt=1743618202814" alt="Placeholder Image" width="98%" style="border-radius: 10px;">
</div>

The system moves your data through a structured path from raw comments to processed insights, ensuring nothing gets lost along the way. Airflow orchestrates this entire workflow while Docker keeps everything running smoothly regardless of your setup.

## Technical Architecture

Our data collection layer connects directly to the YouTube Data API with intelligent polling mechanisms. These connectors respect rate limits while maximizing data throughput to ensure comprehensive comment capture from all your videos without missing engagement.

<div  align="center">
  <img src="https://ik.imagekit.io/fcaqoy5tdf/WhatsApp%20Image%202025-04-05%20at%2013.49.05_46820c00.jpg?updatedAt=1743841178601" alt="Placeholder Image" width="98%" style="border-radius: 10px;width: 632px;height: 400px;">
</div>

At the heart of Whelm sits our NLP core. Unlike general-purpose sentiment tools, our model understand YouTube-specific language patterns, including abbreviated speech, emojis, and platform-specific references that traditional systems miss.

### Technology Stack
- **Orchestration**: Apache Airflow
- **Storage**: MinIO (S3-compatible object storage)
- **Databases**: CockroachDB and PostgreSQL
- **Processing**: PySpark for data transformations
- **ML Models**: 
  - RoBERTa for sentiment analysis
  - Mistral AI for natural language summary generation
- **Containerization**: Docker

### MinIO Bucket Structure
```
├── stage       # Raw data
├── preprocessed # Cleaned data
├── curated     # Data with sentiment scores
├── processed   # Data with summaries
└── dump        # Archived data
```
