from pathlib import Path
from helperfiles.clustering import HouseholdClustering
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def main():
    # Setup paths
    DATA_FILE = Path("merged_data_hourly_with_weather.csv")
    PLOTS_DIR = Path("findings/plots/clustering")
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR = Path("findings/clustering_results")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("="*60)
    print("RUNNING CLUSTERING PIPELINE")
    print("="*60)

    # 1. Init & Load
    clusterer = HouseholdClustering(DATA_FILE)
    
    # 2. Extract Features
    clusterer.extract_features()
    
    # 3. Find Optimal K (Optional - takes time)
    print("\nStep 3: Finding optimal K...")
    clusterer.find_optimal_k(max_k=8, plots_dir=PLOTS_DIR)
    
    # 4. Run Final Clustering (Let's pick k=4 for now, or decision based on step 3)
    # Usually 4-5 is good for energy (Heating, Cooling, Steady, Irregular)
    k = 4
    print(f"\nStep 4: Clustering with k={k}...")
    clusterer.perform_clustering(k=k)
    
    # 5. Save & Plot
    print("\nStep 5: Saving results...")
    clusterer.save_results(RESULTS_DIR / "meter_clusters.csv")
    clusterer.plot_clusters(PLOTS_DIR)
    
    print("\nGenerating Centroid Profiles...")
    # Add clusters back to main DF to plot average profiles
    # This is memory intensive so we do it carefully
    
    # Get just the necessary columns again
    df = clusterer.df[['meter_id', 'hour', 'consumption']].copy()
    clusters = clusterer.features_df[['cluster']]
    
    # Merge clusters onto time series
    df['meter_id'] = df['meter_id'].astype(str)
    # The feature index is the meter_id
    clusters.index = clusters.index.astype(str)
    
    df = df.merge(clusters, left_on='meter_id', right_index=True)
    
    # Plot average daily profile per cluster
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='hour', y='consumption', hue='cluster', palette='viridis', errorbar=None)
    plt.title("Average Daily Consumption Profile per Cluster")
    plt.grid(True, alpha=0.3)
    plt.savefig(PLOTS_DIR / "cluster_daily_profiles.png")
    plt.close()
    
    print(f"Done! Check {PLOTS_DIR} for images.")

if __name__ == "__main__":
    main()
