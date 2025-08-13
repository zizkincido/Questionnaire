import pandas as pd
from datetime import datetime
from collections import defaultdict, Counter

def analyze_music_logs(file_path=None, target_date="10/08/2016"):
    df = pd.read_csv(file_path, sep='\t')

    
    # Extract date from PLAY_TS (handle both date-only and datetime formats)
    def extract_date(timestamp):
        if pd.isna(timestamp):
            return None
        timestamp_str = str(timestamp).strip()
        if len(timestamp_str) >= 10:
            return timestamp_str[:10]
        return timestamp_str
    
    df['DATE'] = df['PLAY_TS'].apply(extract_date)
    
    # Filter for the target date
    target_df = df[df['DATE'] == target_date].copy()
    
    print(f"Found {len(target_df)} records for {target_date}")
    print(f"Records for {target_date}:")
    print(target_df[['CLIENT_ID', 'SONG_ID', 'PLAY_TS']].to_string(index=False))
    print()
    
    # Group by CLIENT_ID and count distinct SONG_ID for each client
    client_distinct_songs = target_df.groupby('CLIENT_ID')['SONG_ID'].nunique().reset_index()
    client_distinct_songs.columns = ['CLIENT_ID', 'DISTINCT_SONGS']
    
    print("Distinct songs per client:")
    print(client_distinct_songs.to_string(index=False))
    print()
    
    # Create distribution table: count how many clients have each distinct song count
    distribution = client_distinct_songs['DISTINCT_SONGS'].value_counts().reset_index()
    distribution.columns = ['DISTINCT_PLAY_COUNT', 'CLIENT_COUNT']
    distribution = distribution.sort_values('DISTINCT_PLAY_COUNT')
    
    return distribution

def save_results(distribution_df, output_file="output.csv", delimiter=","):
    distribution_df.to_csv(output_file, sep=delimiter, index=False)
    print(f"Results saved to {output_file}")

# Main execution
if __name__ == "__main__":
    print("Music Streaming Log Analyzer")
    print("=" * 40)
    
    result = analyze_music_logs(file_path="exhibitA-input.csv", target_date="10/08/2016")
    
    print("Final Distribution Table:")
    print("=" * 25)
    print(result.to_string(index=False))
    
    # CSV output
    save_results(result, "music_analysis_output.csv", delimiter=",")
    
    print("\nAnalysis complete!")