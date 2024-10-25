# Metropolitan Museum Visual Art Explorer

A visual search engine for exploring the Metropolitan Museum of Art's collection through visual similarity. Users can discover historical artwork by navigating through visually similar pieces, powered by deep learning embeddings.

## Overview

This project creates an interactive visual search experience for the Met's open access collection. Users start with randomly selected artworks and can explore the collection by clicking on pieces they find interesting, which shows them visually similar works.

### Key Features
- Visual similarity search using deep learning embeddings
- Exploration-focused interface with random initial seeding
- Integration with Met Museum's Open Access API
- Simple, clean UI focused on the artwork

## Architecture

### 1. Data Pipeline
- Fetch artwork data and images from Met API
- Generate embeddings for each artwork using CLIP
- Store embeddings and metadata in a vector database
- Cache processed images for quick retrieval

### 2. Backend
- FastAPI server handling:
  - Random artwork selection
  - Similarity search queries
  - Metadata retrieval
- Vector database (e.g., Pinecone, Milvus) for efficient similarity search
- Redis cache for frequently accessed images and metadata

### 3. Frontend
- React SPA with:
  - Grid layout for artwork display
  - Click handling for similarity navigation
  - Basic artwork information display
  - Responsive design for different screen sizes

## Technical Stack

- **Frontend**: React, Tailwind CSS
- **Backend**: FastAPI, Python
- **Machine Learning**: CLIP (OpenAI)
- **Database**: Vector DB (Pinecone/Milvus), Redis
- **Infrastructure**: Docker, Cloud Storage (images)

## Getting Started

### Prerequisites
- Python 3.8+
- Node.js 16+
- Docker
- Access to Met Museum API (free)
- Vector database account (Pinecone free tier works fine)

### Initial Setup

1. Clone the repository
```bash
git clone https://github.com/yourusername/met-visual-search
cd met-visual-search
```

2. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

3. Start the development environment
```bash
docker-compose up -d
```

### Data Processing

1. Fetch artwork data
```bash
python scripts/fetch_met_data.py
```

2. Generate embeddings
```bash
python scripts/generate_embeddings.py
```

3. Load embeddings into vector DB
```bash
python scripts/load_vectors.py
```

## Project Structure
```
met-visual-search/
├── backend/           # FastAPI server
├── frontend/         # React frontend
├── scripts/         # Data processing scripts
├── models/          # ML model utilities
├── docker/          # Docker configuration
└── docs/            # Additional documentation
```

## Next Steps & Future Improvements

1. Text search functionality
2. Filtering by period, medium, culture
3. User collections and favorites
4. Advanced visualization of the embedding space
5. Performance optimizations for larger dataset

## Contributing

Contributions are welcome! Please read our contributing guidelines and code of conduct before submitting PRs.

## License

MIT License - see LICENSE.md

## Acknowledgments

- Metropolitan Museum of Art for their Open Access Program
- OpenAI for CLIP
- Inspired by Same Energy visual search engine