# Datacentre API

A FastAPI-based service that provides access to Soccerverse game data.

## Features

- Access to game data including clubs, players, users, and market information
- FastAPI with automatic Swagger documentation
- Async database connections for optimal performance
- Detailed filtering capabilities for all endpoints

## Setup

1. Clone this repository
2. Create a `.env` file based on `example.env`
3. Install dependencies: `pip install -r requirements.txt`
4. Run the service: `python main.py`

## Available Endpoints

This API provides access to the following main endpoints:

- `/clubs` - Basic club data with market information
- `/clubs/detailed` - Detailed club information
- `/clubs/{club_id}/job` - Job posting details for a specific club
- `/players` - Access player data
- `/users` - Access user data
- `/share_balances` - Access influence ownership data
- `/share_trade_history` - Access trading history
- And many more...

## Club Job Postings

The API now supports retrieving job posting information:

- Basic job posting info (posted_at and poster_name) is included in `/clubs/detailed`
- Full job details are available via `/clubs/{club_id}/job` endpoint
- When querying a specific club by ID, job description and top influencers are included
- Job posting data can be filtered with `has_job_posting=1` parameter
- Profile pictures are fetched for poster and top influencers

## Implementation Notes

### Profile Pictures

Profile pictures are handled by the `get_profiles_for_users` utility function which:

- Supports both string and bytes usernames for cross-environment compatibility
- Retrieves profile pictures from the `usernames` table in the userconfig database
- Applies proper profile visibility rules based on user settings
- Returns the configured default profile picture when needed

## Testing

For running tests, see the [testing README](tests/README.md).

## Documentation

FastAPI generates interactive API documentation automatically:

- OpenAPI documentation: `/docs`
- ReDoc documentation: `/redoc`