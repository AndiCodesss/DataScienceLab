import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import gc
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Configuration
DATA_FILE = Path("merged_data_hourly_with_weather.csv")
PLOTS_DIR = Path("findings/plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Set style for better-looking plots
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def load_data():
    """Load and prepare the merged dataset with optimized dtypes."""
    print("=" * 80)
    print("LOADING DATA")
    print("=" * 80)

    # Optimize types for memory efficiency
    dtypes = {
        'meter_id': 'category',
        'zip_code': 'category',
        'sk_region_code': 'category',
        'region_name': 'category',
        'region_city': 'category',
        'weekday': 'category',
        'weather_condition': 'float32',
        'consumption': 'float32',
        'temperature': 'float32',
        'dew_point': 'float32',
        'relative_humidity': 'float32',
        'wind_speed': 'float32',
        'precipitation': 'float32',
        'snow_depth': 'float32',
        'pressure': 'float32',
        'is_weekend': 'int8',
        'is_holiday': 'int8',
        'hour': 'int8',
        'month': 'int8',
        'day_of_month': 'int8'
    }

    print(f"Reading {DATA_FILE}...")
    print("This may take a moment due to file size (2.2GB)...")

    # Read CSV with optimized types
    df = pd.read_csv(DATA_FILE, dtype=dtypes, parse_dates=['timestamp', 'date'])

    print(f"\n✓ Successfully loaded {len(df):,} rows")
    print(f"✓ Data spans from {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"✓ Total unique meters: {df['meter_id'].nunique()}")
    print(f"✓ Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    return df

def basic_statistics(df):
    """Generate comprehensive basic statistics."""
    print("\n" + "=" * 80)
    print("BASIC STATISTICS")
    print("=" * 80)

    # Dataset overview
    print("\n1. DATASET OVERVIEW:")
    print("-" * 40)
    print(f"Total records: {len(df):,}")
    print(f"Unique meters: {df['meter_id'].nunique()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    days_covered = (df['date'].max() - df['date'].min()).days + 1
    print(f"Days covered: {days_covered}")
    print(f"Expected records (1000 meters × {days_covered} days × 24 hours): {1000 * days_covered * 24:,}")
    completeness = (len(df) / (1000 * days_covered * 24)) * 100
    print(f"Data completeness: {completeness:.2f}%")

    # Consumption statistics
    print("\n2. CONSUMPTION STATISTICS:")
    print("-" * 40)
    consumption_stats = df['consumption'].describe()
    print(f"Mean consumption: {consumption_stats['mean']:.3f} kWh")
    print(f"Median consumption: {consumption_stats['50%']:.3f} kWh")
    print(f"Std deviation: {consumption_stats['std']:.3f} kWh")
    print(f"Min consumption: {consumption_stats['min']:.3f} kWh")
    print(f"Max consumption: {consumption_stats['max']:.3f} kWh")
    print(f"25th percentile: {consumption_stats['25%']:.3f} kWh")
    print(f"75th percentile: {consumption_stats['75%']:.3f} kWh")

    # Check for anomalies
    zero_consumption = (df['consumption'] == 0).sum()
    negative_consumption = (df['consumption'] < 0).sum()
    print(f"\nAnomaly Detection:")
    print(f"  - Zero consumption records: {zero_consumption:,} ({zero_consumption/len(df)*100:.2f}%)")
    print(f"  - Negative consumption records: {negative_consumption:,}")

    # Missing data analysis
    print("\n3. MISSING DATA ANALYSIS:")
    print("-" * 40)
    missing_cols = ['consumption', 'temperature', 'relative_humidity', 'wind_speed', 'precipitation']
    for col in missing_cols:
        if col in df.columns:
            missing = df[col].isna().sum()
            pct = (missing / len(df)) * 100
            print(f"{col:20s}: {missing:8,} missing ({pct:.2f}%)")

    # Regional distribution
    print("\n4. REGIONAL DISTRIBUTION:")
    print("-" * 40)
    if 'region_name' in df.columns:
        region_counts = df.groupby('region_name')['meter_id'].nunique().sort_values(ascending=False)
        print("Meters per region:")
        for region, count in region_counts.items():
            print(f"  {region}: {count} meters")

    return consumption_stats

def analyze_temporal_patterns(df):
    """Analyze temporal consumption patterns in detail."""
    print("\n" + "=" * 80)
    print("TEMPORAL PATTERN ANALYSIS")
    print("=" * 80)

    # Hourly patterns
    print("\n1. HOURLY PATTERNS:")
    print("-" * 40)
    hourly_avg = df.groupby('hour')['consumption'].agg(['mean', 'std', 'median'])
    peak_hour = hourly_avg['mean'].idxmax()
    valley_hour = hourly_avg['mean'].idxmin()

    print(f"Peak consumption hour: {peak_hour}:00 ({hourly_avg.loc[peak_hour, 'mean']:.3f} kWh)")
    print(f"Valley consumption hour: {valley_hour}:00 ({hourly_avg.loc[valley_hour, 'mean']:.3f} kWh)")
    peak_valley_ratio = hourly_avg.loc[peak_hour, 'mean'] / hourly_avg.loc[valley_hour, 'mean']
    print(f"Peak-to-valley ratio: {peak_valley_ratio:.2f}x")

    # Morning and evening peaks
    morning_hours = df[df['hour'].between(6, 9)]
    evening_hours = df[df['hour'].between(17, 21)]
    print(f"\nMorning peak (6-9 AM) average: {morning_hours['consumption'].mean():.3f} kWh")
    print(f"Evening peak (5-9 PM) average: {evening_hours['consumption'].mean():.3f} kWh")

    # Plot hourly profile with confidence intervals
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # Average with error bars
    ax1.errorbar(hourly_avg.index, hourly_avg['mean'], yerr=hourly_avg['std'],
                 marker='o', capsize=5, capthick=2)
    ax1.set_xlabel('Hour of Day')
    ax1.set_ylabel('Consumption (kWh)')
    ax1.set_title('Average Hourly Consumption Profile with Standard Deviation')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(24))

    # Median vs Mean comparison
    ax2.plot(hourly_avg.index, hourly_avg['mean'], marker='o', label='Mean', linewidth=2)
    ax2.plot(hourly_avg.index, hourly_avg['median'], marker='s', label='Median', linewidth=2)
    ax2.set_xlabel('Hour of Day')
    ax2.set_ylabel('Consumption (kWh)')
    ax2.set_title('Mean vs Median Hourly Consumption')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(range(24))

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "hourly_analysis_detailed.png", dpi=150)
    plt.close()

    # Day of week patterns
    print("\n2. WEEKLY PATTERNS:")
    print("-" * 40)
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekly_avg = df.groupby('weekday')['consumption'].agg(['mean', 'std']).reindex(day_order)

    for day in day_order:
        print(f"{day:10s}: {weekly_avg.loc[day, 'mean']:.3f} kWh (±{weekly_avg.loc[day, 'std']:.3f})")

    weekday_avg = df[df['is_weekend'] == 0]['consumption'].mean()
    weekend_avg = df[df['is_weekend'] == 1]['consumption'].mean()
    print(f"\nWeekday average: {weekday_avg:.3f} kWh")
    print(f"Weekend average: {weekend_avg:.3f} kWh")
    print(f"Weekend vs Weekday ratio: {weekend_avg/weekday_avg:.2%}")

    # Monthly patterns
    print("\n3. MONTHLY/SEASONAL PATTERNS:")
    print("-" * 40)
    monthly_avg = df.groupby('month')['consumption'].agg(['mean', 'std', 'median'])
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    for month in monthly_avg.index:
        if month <= len(month_names):
            print(f"{month_names[month-1]:3s}: {monthly_avg.loc[month, 'mean']:.3f} kWh "
                  f"(±{monthly_avg.loc[month, 'std']:.3f})")

    # Identify seasons
    winter_months = [1, 2, 12]
    summer_months = [6, 7, 8]
    winter_avg = df[df['month'].isin(winter_months)]['consumption'].mean()
    summer_avg = df[df['month'].isin(summer_months)]['consumption'].mean()
    print(f"\nWinter average (Dec-Feb): {winter_avg:.3f} kWh")
    print(f"Summer average (Jun-Aug): {summer_avg:.3f} kWh")
    print(f"Winter/Summer ratio: {winter_avg/summer_avg:.2f}x")

    # Holiday effects
    if 'is_holiday' in df.columns:
        print("\n4. HOLIDAY EFFECTS:")
        print("-" * 40)
        holiday_consumption = df[df['is_holiday'] == 1]['consumption'].mean()
        non_holiday_consumption = df[df['is_holiday'] == 0]['consumption'].mean()
        print(f"Holiday average: {holiday_consumption:.3f} kWh")
        print(f"Non-holiday average: {non_holiday_consumption:.3f} kWh")
        print(f"Holiday effect: {(holiday_consumption/non_holiday_consumption - 1)*100:.2f}%")

    return hourly_avg, weekly_avg, monthly_avg

def analyze_weather_impact(df):
    """Analyze the impact of weather on consumption."""
    print("\n" + "=" * 80)
    print("WEATHER IMPACT ANALYSIS")
    print("=" * 80)

    weather_cols = ['temperature', 'relative_humidity', 'wind_speed', 'precipitation',
                    'snow_depth', 'pressure', 'dew_point']
    existing_weather_cols = [col for col in weather_cols if col in df.columns]

    if not existing_weather_cols:
        print("No weather data available for analysis.")
        return

    # Correlation analysis
    print("\n1. CORRELATION WITH WEATHER VARIABLES:")
    print("-" * 40)

    correlations = {}
    for col in existing_weather_cols:
        if df[col].notna().sum() > 0:  # Check if we have data
            corr = df[['consumption', col]].dropna().corr().iloc[0, 1]
            correlations[col] = corr
            print(f"{col:20s}: {corr:+.4f}")

    # Temperature analysis (if available)
    if 'temperature' in df.columns and df['temperature'].notna().sum() > 0:
        print("\n2. TEMPERATURE IMPACT ANALYSIS:")
        print("-" * 40)

        # Create temperature bins
        temp_bins = pd.cut(df['temperature'].dropna(), bins=10)
        temp_consumption = df.groupby(temp_bins)['consumption'].mean()

        print("Consumption by temperature range:")
        for temp_range, consumption in temp_consumption.items():
            if pd.notna(consumption):
                print(f"  {temp_range}: {consumption:.3f} kWh")

        # Find optimal temperature
        temp_df = df[['temperature', 'consumption']].dropna()
        if len(temp_df) > 0:
            # Polynomial fit to find minimum consumption temperature
            z = np.polyfit(temp_df['temperature'], temp_df['consumption'], 2)
            p = np.poly1d(z)

            if z[0] > 0:  # U-shaped curve
                optimal_temp = -z[1] / (2 * z[0])
                print(f"\nOptimal temperature (lowest consumption): {optimal_temp:.1f}°C")

            # Extreme temperature analysis
            cold_threshold = temp_df['temperature'].quantile(0.1)
            hot_threshold = temp_df['temperature'].quantile(0.9)

            cold_consumption = temp_df[temp_df['temperature'] <= cold_threshold]['consumption'].mean()
            mild_consumption = temp_df[(temp_df['temperature'] > cold_threshold) &
                                       (temp_df['temperature'] < hot_threshold)]['consumption'].mean()
            hot_consumption = temp_df[temp_df['temperature'] >= hot_threshold]['consumption'].mean()

            print(f"\nExtreme weather impact:")
            print(f"  Cold days (<{cold_threshold:.1f}°C): {cold_consumption:.3f} kWh")
            print(f"  Mild days: {mild_consumption:.3f} kWh")
            print(f"  Hot days (>{hot_threshold:.1f}°C): {hot_consumption:.3f} kWh")

    # Humidity impact
    if 'relative_humidity' in df.columns and df['relative_humidity'].notna().sum() > 0:
        print("\n3. HUMIDITY IMPACT:")
        print("-" * 40)
        humidity_bins = pd.cut(df['relative_humidity'].dropna(), bins=[0, 30, 60, 80, 100],
                               labels=['Low (<30%)', 'Medium (30-60%)', 'High (60-80%)', 'Very High (>80%)'])
        humidity_consumption = df.groupby(humidity_bins)['consumption'].mean()

        for humidity_range, consumption in humidity_consumption.items():
            if pd.notna(consumption):
                print(f"  {humidity_range}: {consumption:.3f} kWh")

    # Precipitation impact
    if 'precipitation' in df.columns:
        non_null_precip = df['precipitation'].notna().sum()
        if non_null_precip > 0:
            print("\n4. PRECIPITATION IMPACT:")
            print("-" * 40)
            print(f"Available precipitation data: {non_null_precip:,} records ({non_null_precip/len(df)*100:.2f}%)")

            no_rain = df[df['precipitation'] == 0]['consumption'].mean()
            rain = df[df['precipitation'] > 0]['consumption'].mean()
            heavy_rain_threshold = df['precipitation'].quantile(0.9)
            heavy_rain = df[df['precipitation'] > heavy_rain_threshold]['consumption'].mean()

            if pd.notna(no_rain):
                print(f"No precipitation: {no_rain:.3f} kWh")
            else:
                print(f"No precipitation: No data available")

            if pd.notna(rain):
                print(f"With precipitation: {rain:.3f} kWh")
            else:
                print(f"With precipitation: No data available")

            if pd.notna(heavy_rain):
                print(f"Heavy precipitation: {heavy_rain:.3f} kWh")
            else:
                print(f"Heavy precipitation: No data available")

            if pd.notna(no_rain) and pd.notna(rain) and no_rain != 0:
                print(f"Rain impact: {(rain/no_rain - 1)*100:+.2f}%")
            else:
                print(f"Rain impact: Cannot calculate (insufficient data)")
        else:
            print("\n4. PRECIPITATION IMPACT:")
            print("-" * 40)
            print("Insufficient precipitation data available for analysis")

    # Create weather impact visualization
    if len(existing_weather_cols) >= 2:
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.ravel()

        plot_idx = 0
        for col in existing_weather_cols[:4]:
            if df[col].notna().sum() > 1000 and plot_idx < 4:  # Only plot if sufficient data
                # Sample data for faster plotting
                clean_data = df[[col, 'consumption']].dropna()
                sample_size = min(10000, len(clean_data))
                if sample_size > 0:
                    sample = clean_data.sample(sample_size)
                    axes[plot_idx].scatter(sample[col], sample['consumption'], alpha=0.3, s=1)

                    # Add trend line
                    z = np.polyfit(sample[col], sample['consumption'], 2)
                    p = np.poly1d(z)
                    x_line = np.linspace(sample[col].min(), sample[col].max(), 100)
                    axes[plot_idx].plot(x_line, p(x_line), 'r-', linewidth=2)

                    axes[plot_idx].set_xlabel(col.replace('_', ' ').title())
                    axes[plot_idx].set_ylabel('Consumption (kWh)')
                    axes[plot_idx].set_title(f'Consumption vs {col.replace("_", " ").title()}')
                    axes[plot_idx].grid(True, alpha=0.3)
                    plot_idx += 1

        # Hide unused axes
        for idx in range(plot_idx, 4):
            axes[idx].set_visible(False)

        plt.tight_layout()
        plt.savefig(PLOTS_DIR / "weather_impact_analysis.png", dpi=150)
        plt.close()

    return correlations

def analyze_household_behavior(df):
    """Deep dive into household consumption patterns."""
    print("\n" + "=" * 80)
    print("HOUSEHOLD BEHAVIOR ANALYSIS")
    print("=" * 80)

    # Calculate metrics per household
    print("\n1. CALCULATING HOUSEHOLD METRICS...")
    print("-" * 40)

    # Daily consumption per household
    daily_consumption = df.groupby(['meter_id', 'date'], observed=True)['consumption'].sum().reset_index()

    # Household statistics
    household_stats = daily_consumption.groupby('meter_id', observed=True)['consumption'].agg([
        'mean', 'std', 'median', 'min', 'max', 'count'
    ]).round(3)

    print(f"Analyzed {len(household_stats)} households")
    print(f"Average days of data per household: {household_stats['count'].mean():.1f}")

    # Identify different consumer types
    print("\n2. CONSUMER SEGMENTATION:")
    print("-" * 40)

    # Define consumption categories based on quartiles
    q1 = household_stats['mean'].quantile(0.25)
    q2 = household_stats['mean'].quantile(0.50)
    q3 = household_stats['mean'].quantile(0.75)

    low_consumers = household_stats[household_stats['mean'] <= q1]
    medium_low = household_stats[(household_stats['mean'] > q1) & (household_stats['mean'] <= q2)]
    medium_high = household_stats[(household_stats['mean'] > q2) & (household_stats['mean'] <= q3)]
    high_consumers = household_stats[household_stats['mean'] > q3]

    print(f"Low consumers (≤{q1:.2f} kWh/day): {len(low_consumers)} households")
    print(f"Medium-low ({q1:.2f}-{q2:.2f} kWh/day): {len(medium_low)} households")
    print(f"Medium-high ({q2:.2f}-{q3:.2f} kWh/day): {len(medium_high)} households")
    print(f"High consumers (>{q3:.2f} kWh/day): {len(high_consumers)} households")

    # Top and bottom consumers
    print("\n3. EXTREME CONSUMERS:")
    print("-" * 40)

    top_10 = household_stats.nlargest(10, 'mean')
    bottom_10 = household_stats.nsmallest(10, 'mean')

    print("Top 10 highest consumers (daily average):")
    for meter_id, row in top_10.iterrows():
        print(f"  Meter {meter_id}: {row['mean']:.2f} kWh/day (std: {row['std']:.2f})")

    print("\nBottom 10 lowest consumers (daily average):")
    for meter_id, row in bottom_10.iterrows():
        print(f"  Meter {meter_id}: {row['mean']:.2f} kWh/day (std: {row['std']:.2f})")

    # Variability analysis
    print("\n4. CONSUMPTION VARIABILITY:")
    print("-" * 40)

    # Calculate coefficient of variation
    household_stats['cv'] = household_stats['std'] / household_stats['mean']

    high_variability = household_stats[household_stats['cv'] > household_stats['cv'].quantile(0.9)]
    low_variability = household_stats[household_stats['cv'] < household_stats['cv'].quantile(0.1)]

    print(f"Average coefficient of variation: {household_stats['cv'].mean():.3f}")
    print(f"Households with high variability (top 10%): {len(high_variability)}")
    print(f"Households with low variability (bottom 10%): {len(low_variability)}")

    # Identify potential issues
    print("\n5. DATA QUALITY ISSUES:")
    print("-" * 40)

    zero_consumption_meters = daily_consumption[daily_consumption['consumption'] == 0].groupby('meter_id').size()
    always_zero = household_stats[household_stats['max'] == 0]

    print(f"Meters with at least one zero-consumption day: {len(zero_consumption_meters)}")
    print(f"Meters with ONLY zero consumption: {len(always_zero)}")

    if len(always_zero) > 0:
        print(f"  Problematic meters: {list(always_zero.index[:10])}")

    # Create comprehensive household visualization
    fig = plt.figure(figsize=(16, 12))

    # 1. Distribution of average daily consumption
    ax1 = plt.subplot(2, 3, 1)
    ax1.hist(household_stats['mean'], bins=50, edgecolor='black', alpha=0.7)
    ax1.axvline(household_stats['mean'].median(), color='red', linestyle='--', label='Median')
    ax1.axvline(household_stats['mean'].mean(), color='blue', linestyle='--', label='Mean')
    ax1.set_xlabel('Average Daily Consumption (kWh)')
    ax1.set_ylabel('Number of Households')
    ax1.set_title('Distribution of Household Consumption')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 2. Consumption vs Variability
    ax2 = plt.subplot(2, 3, 2)
    ax2.scatter(household_stats['mean'], household_stats['cv'], alpha=0.5, s=20)
    ax2.set_xlabel('Average Daily Consumption (kWh)')
    ax2.set_ylabel('Coefficient of Variation')
    ax2.set_title('Consumption vs Variability')
    ax2.grid(True, alpha=0.3)

    # 3. Box plot by consumption category
    ax3 = plt.subplot(2, 3, 3)
    consumption_categories = []
    for meter_id in household_stats.index:
        mean_val = household_stats.loc[meter_id, 'mean']
        if mean_val <= q1:
            consumption_categories.append('Low')
        elif mean_val <= q2:
            consumption_categories.append('Medium-Low')
        elif mean_val <= q3:
            consumption_categories.append('Medium-High')
        else:
            consumption_categories.append('High')

    household_stats['category'] = consumption_categories
    household_stats.boxplot(column='mean', by='category', ax=ax3)
    ax3.set_xlabel('Consumer Category')
    ax3.set_ylabel('Daily Consumption (kWh)')
    ax3.set_title('Consumption by Category')
    plt.sca(ax3)
    plt.xticks(rotation=45)

    # 4. Cumulative distribution
    ax4 = plt.subplot(2, 3, 4)
    sorted_means = household_stats['mean'].sort_values()
    cumulative = np.arange(1, len(sorted_means) + 1) / len(sorted_means) * 100
    ax4.plot(sorted_means, cumulative)
    ax4.set_xlabel('Daily Consumption (kWh)')
    ax4.set_ylabel('Cumulative % of Households')
    ax4.set_title('Cumulative Distribution')
    ax4.grid(True, alpha=0.3)

    # 5. Time consistency (standard deviation)
    ax5 = plt.subplot(2, 3, 5)
    ax5.hist(household_stats['std'], bins=50, edgecolor='black', alpha=0.7)
    ax5.set_xlabel('Standard Deviation of Daily Consumption')
    ax5.set_ylabel('Number of Households')
    ax5.set_title('Consumption Variability Distribution')
    ax5.grid(True, alpha=0.3)

    # 6. Top vs Bottom consumers over time
    ax6 = plt.subplot(2, 3, 6)
    top_5_meters = household_stats.nlargest(5, 'mean').index
    bottom_5_meters = household_stats.nsmallest(5, 'mean').index

    for meter in top_5_meters:
        meter_data = daily_consumption[daily_consumption['meter_id'] == meter]
        ax6.plot(meter_data['date'], meter_data['consumption'], alpha=0.7, linewidth=0.5, color='red')

    for meter in bottom_5_meters:
        meter_data = daily_consumption[daily_consumption['meter_id'] == meter]
        ax6.plot(meter_data['date'], meter_data['consumption'], alpha=0.7, linewidth=0.5, color='blue')

    ax6.set_xlabel('Date')
    ax6.set_ylabel('Daily Consumption (kWh)')
    ax6.set_title('Top 5 (red) vs Bottom 5 (blue) Consumers')
    ax6.grid(True, alpha=0.3)
    plt.setp(ax6.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "household_analysis_comprehensive.png", dpi=150)
    plt.close()

    # Save detailed statistics
    stats_file = PLOTS_DIR / "household_statistics.csv"
    household_stats.to_csv(stats_file)
    print(f"\n✓ Detailed household statistics saved to: {stats_file}")

    return household_stats

def analyze_regional_patterns(df):
    """Analyze regional consumption patterns and differences."""
    print("\n" + "=" * 80)
    print("REGIONAL ANALYSIS")
    print("=" * 80)

    if 'region_name' not in df.columns:
        print("No regional data available for analysis.")
        return

    # Regional summary
    print("\n1. REGIONAL CONSUMPTION SUMMARY:")
    print("-" * 40)

    regional_stats = df.groupby('region_name', observed=True)['consumption'].agg([
        'mean', 'std', 'median', 'sum'
    ]).round(3)

    regional_stats['total_kwh'] = regional_stats['sum']
    regional_stats['pct_of_total'] = (regional_stats['sum'] / regional_stats['sum'].sum() * 100).round(2)

    regional_stats_sorted = regional_stats.sort_values('mean', ascending=False)

    for region, row in regional_stats_sorted.iterrows():
        print(f"{region:25s}: {row['mean']:.3f} kWh avg "
              f"({row['pct_of_total']:.1f}% of total)")

    # Regional household count
    print("\n2. HOUSEHOLDS PER REGION:")
    print("-" * 40)

    households_per_region = df.groupby('region_name', observed=True)['meter_id'].nunique()
    for region, count in households_per_region.sort_values(ascending=False).items():
        print(f"{region:25s}: {count} households")

    # Weather differences by region (if coordinates available)
    if 'temperature' in df.columns:
        print("\n3. REGIONAL WEATHER PATTERNS:")
        print("-" * 40)

        regional_weather = df.groupby('region_name', observed=True)['temperature'].agg(['mean', 'std'])
        regional_weather_sorted = regional_weather.sort_values('mean')

        for region, row in regional_weather_sorted.iterrows():
            print(f"{region:25s}: {row['mean']:.1f}°C average temperature")

        coldest = regional_weather_sorted.index[0]
        warmest = regional_weather_sorted.index[-1]
        print(f"\nColdest region: {coldest}")
        print(f"Warmest region: {warmest}")

    # Income impact (if available)
    if 'households_income_bracket_5' in df.columns:  # Check for middle income bracket
        print("\n4. INCOME DISTRIBUTION IMPACT:")
        print("-" * 40)

        # Get unique income data per region
        income_cols = [col for col in df.columns if 'households_income_bracket' in col]
        if income_cols:
            regional_income = df.groupby('region_name', observed=True)[income_cols].first()

            # Calculate weighted average income bracket
            for region in regional_income.index:
                print(f"{region}: Income data available")

    # Peak hours by region
    print("\n5. PEAK CONSUMPTION HOURS BY REGION:")
    print("-" * 40)

    for region in df['region_name'].unique():
        if pd.notna(region):
            region_data = df[df['region_name'] == region]
            hourly_avg = region_data.groupby('hour')['consumption'].mean()
            peak_hour = hourly_avg.idxmax()
            print(f"{region:25s}: Peak at {peak_hour}:00")

    # Create regional visualizations
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    # 1. Average consumption by region
    ax1 = axes[0, 0]
    regional_stats_sorted['mean'].plot(kind='barh', ax=ax1, color='skyblue')
    ax1.set_xlabel('Average Consumption (kWh)')
    ax1.set_title('Average Hourly Consumption by Region')
    ax1.grid(True, alpha=0.3)

    # 2. Regional consumption distribution
    ax2 = axes[0, 1]
    regions_for_box = df.groupby('region_name', observed=True)['consumption'].apply(list).to_dict()
    box_data = [data for region, data in regions_for_box.items() if len(data) > 0]
    box_labels = [region for region, data in regions_for_box.items() if len(data) > 0]

    bp = ax2.boxplot(box_data, labels=box_labels, patch_artist=True)
    ax2.set_xlabel('Region')
    ax2.set_ylabel('Consumption (kWh)')
    ax2.set_title('Consumption Distribution by Region')
    ax2.tick_params(axis='x', rotation=45)
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 3. Households per region
    ax3 = axes[1, 0]
    households_per_region.sort_values().plot(kind='barh', ax=ax3, color='lightgreen')
    ax3.set_xlabel('Number of Households')
    ax3.set_title('Household Count by Region')
    ax3.grid(True, alpha=0.3)

    # 4. Regional hourly patterns
    ax4 = axes[1, 1]
    for region in df['region_name'].unique()[:5]:  # Top 5 regions
        if pd.notna(region):
            region_data = df[df['region_name'] == region]
            hourly = region_data.groupby('hour')['consumption'].mean()
            ax4.plot(hourly.index, hourly.values, label=region, marker='o', markersize=3)

    ax4.set_xlabel('Hour of Day')
    ax4.set_ylabel('Average Consumption (kWh)')
    ax4.set_title('Hourly Patterns by Region (Top 5)')
    ax4.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax4.grid(True, alpha=0.3)
    ax4.set_xticks(range(0, 24, 2))

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "regional_analysis.png", dpi=150)
    plt.close()

    return regional_stats

def generate_comprehensive_report(df, stats_dict):
    """Generate a comprehensive text report of all findings."""
    report_path = PLOTS_DIR / "comprehensive_eda_report.txt"

    with open(report_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("COMPREHENSIVE EDA REPORT - SLOVAK ENERGY CONSUMPTION ANALYSIS\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Report Generated: {pd.Timestamp.now()}\n")
        f.write(f"Data File: {DATA_FILE}\n")
        f.write(f"Total Records: {len(df):,}\n")
        f.write(f"Date Range: {df['date'].min()} to {df['date'].max()}\n")
        f.write(f"Unique Meters: {df['meter_id'].nunique()}\n\n")

        # Key findings
        f.write("KEY FINDINGS\n")
        f.write("-" * 40 + "\n")

        # Add all collected statistics
        for key, value in stats_dict.items():
            f.write(f"\n{key}:\n")
            if isinstance(value, pd.DataFrame):
                f.write(value.to_string())
            elif isinstance(value, dict):
                for k, v in value.items():
                    f.write(f"  {k}: {v}\n")
            else:
                f.write(f"  {value}\n")
            f.write("\n")

    print(f"\n✓ Comprehensive report saved to: {report_path}")

def main():
    """Main execution function."""
    try:
        # Load data
        df = load_data()

        # Dictionary to store all statistics
        stats_dict = {}

        # Run all analyses
        consumption_stats = basic_statistics(df)
        stats_dict['Basic Statistics'] = consumption_stats

        hourly_avg, weekly_avg, monthly_avg = analyze_temporal_patterns(df)
        stats_dict['Hourly Patterns'] = hourly_avg
        stats_dict['Weekly Patterns'] = weekly_avg
        stats_dict['Monthly Patterns'] = monthly_avg

        weather_correlations = analyze_weather_impact(df)
        stats_dict['Weather Correlations'] = weather_correlations

        household_stats = analyze_household_behavior(df)
        stats_dict['Household Statistics Summary'] = household_stats.describe()

        regional_stats = analyze_regional_patterns(df)
        if regional_stats is not None:
            stats_dict['Regional Statistics'] = regional_stats

        # Generate comprehensive report
        generate_comprehensive_report(df, stats_dict)

        print("\n" + "=" * 80)
        print("EDA COMPLETE!")
        print("=" * 80)
        print(f"\n✓ All visualizations saved to: {PLOTS_DIR.absolute()}")
        print(f"✓ Detailed statistics saved to: {PLOTS_DIR / 'household_statistics.csv'}")
        print(f"✓ Comprehensive report saved to: {PLOTS_DIR / 'comprehensive_eda_report.txt'}")
        print("\nAnalysis completed successfully!")

    except Exception as e:
        print(f"\n✗ ERROR during EDA: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()