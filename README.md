# Chirp - Simple Twitter Clone

## Authors
- Alexis Kueny
- Pierre-Yves Savioz

## Setup and Installation

1. Make sure you have Redis installed and running on localhost:6379
   - For Arch Linux users: `sudo pacman -S redis`
   - Start Redis service: `sudo systemctl start redis`

2. Install required Python packages:
```
pip install redis streamlit
```

Note: The `bz2` and `zipfile` modules are part of the standard Python library and don't need separate installation.

## How to Run

### Data Import

Run the main.py script to import Twitter data from the ZIP file to Redis:

```
python main.py
```

This will:
- Connect to Redis
- Extract JSON data from data2/20221231_23.zip
- Process English tweets only
- Store users and chirps in Redis
- Print statistics about the imported data

### Web Application

Run the Streamlit app to browse the Chirp data:

```
streamlit run chirp_app.py
```

The app will be available at: http://localhost:8501

The web interface allows you to:
- View the latest chirps
- See top users by followers and chirp count
- Simulate posting a new chirp

## Notes
- Data is limited to 10 files from the source to keep processing time reasonable
- Only English tweets are imported
- Make sure Redis server is running before executing the scripts
