import pandas as pd
import random
import datetime
import json
from faker import Faker
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Faker for realistic data
fake = Faker()

class CustomAECDataGenerator:
    def __init__(self):
        self.companies = [
            'Bechtel', 'Kiewit', 'Fluor', 'AECOM', 'Balfour Beatty', 'Skanska', 
            'Turner Construction', 'PCL Construction', 'Jacobs', 'Hochtief', 
            'Vinci', 'China State Construction', 'Lendlease', 'McCarthy', 'Gilbane',
            'Bouygues', 'Laing O’Rourke', 'Clark Construction', 'Whiting-Turner', 
            'Ferrovial', 'Strabag', 'Shimizu Corporation', 'Obayashi Corporation', 
            'Mace Group', 'Mortenson Construction', 'SNC-Lavalin', 'Dragados', 
            'BAM Group', 'Saipem', 'Thiess', 'Larsen & Toubro', 'Samsung C&T', 
            'Hyundai E&C', 'Salini Impregilo', 'Acciona', 'Kajima Corporation', 
            'Multiplex', 'John Holland', 'CPB Contractors', 'Leighton Asia'  # More global/regional players
        ]
        self.sectors = ['Residential', 'Commercial', 'Infrastructure', 'Industrial', 
                        'Energy', 'Healthcare', 'Education', 'Transportation', 'Utilities']
        self.regions = ['North America', 'Europe', 'Asia-Pacific', 'Middle East', 
                        'Africa', 'Latin America', 'Australia']
        self.countries = {
            'North America': ['USA', 'Canada', 'Mexico'],
            'Europe': ['UK', 'Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Sweden', 'Poland'],
            'Asia-Pacific': ['China', 'India', 'Australia', 'Japan', 'Singapore', 'South Korea', 'Indonesia', 'Malaysia'],
            'Middle East': ['UAE', 'Saudi Arabia', 'Qatar', 'Kuwait', 'Oman'],
            'Africa': ['South Africa', 'Nigeria', 'Kenya', 'Egypt', 'Algeria', 'Morocco'],
            'Latin America': ['Brazil', 'Argentina', 'Chile', 'Colombia', 'Peru', 'Venezuela'],
            'Australia': ['Australia']
        }
        self.subregions = {
            'USA': ['California', 'Texas', 'New York', 'Florida', 'Illinois', 'Georgia', 'Washington'],
            'Canada': ['Ontario', 'British Columbia', 'Quebec', 'Alberta'],
            'Australia': ['New South Wales', 'Victoria', 'Queensland', 'Western Australia'],
            'UK': ['England', 'Scotland', 'Wales', 'Northern Ireland'],
            'China': ['Shanghai', 'Beijing', 'Guangdong', 'Jiangsu'],
            'India': ['Maharashtra', 'Delhi', 'Karnataka', 'Tamil Nadu'],
            'Brazil': ['São Paulo', 'Rio de Janeiro', 'Minas Gerais'],
            'South Africa': ['Gauteng', 'Western Cape', 'KwaZulu-Natal']
        }
        self.project_types = [
            'High-rise Building', 'Bridge', 'Highway', 'Airport', 'Power Plant', 
            'Hospital', 'Residential Complex', 'Data Center', 'Railway', 'Stadium',
            'School', 'Port', 'Dam', 'Wind Farm', 'Solar Plant', 'Metro System', 
            'Warehouse', 'University Campus', 'Water Treatment Plant', 'Refinery', 
            'Shopping Mall', 'Oil Pipeline', 'Smart City'
        ]
        self.clients = [
            'Government', 'Private Developer', 'Municipality', 'Corporate', 
            'NGO', 'Public-Private Partnership', 'International Agency', 
            'State-Owned Enterprise', 'Utility Company'
        ]
        self.strategies = [
            'Merger', 'Acquisition', 'Market Expansion', 'Technology Adoption', 
            'Partnership', 'Sustainability Initiative', 'Cost Optimization', 
            'Digital Transformation', 'Workforce Development', 'Risk Management'
        ]
        self.esg_focus = [
            'Green Building', 'Carbon Neutrality', 'Waste Reduction', 
            'Community Engagement', 'Renewable Energy', 'Water Conservation', 'None'
        ]
        self.risk_factors = [
            'Supply Chain Delays', 'Regulatory Changes', 'Labor Shortages', 
            'Cost Overruns', 'Geopolitical Risks', 'Environmental Concerns', 'None'
        ]
        self.procurement_types = ['Open Bid', 'Selective Bid', 'Negotiated Contract', 
                                 'Design-Build', 'EPC']
        self.contract_types = ['Fixed-Price', 'Cost-Plus', 'Time and Materials', 
                               'Unit Price', 'Lump Sum']
        self.completion_status = ['On Track', 'Delayed', 'Completed', 'On Hold', 'Cancelled']
        self.risk_levels = ['Low', 'Medium', 'High']
        self.strategic_priorities = ['Growth', 'Innovation', 'Sustainability', 
                                    'Efficiency', 'Market Leadership']
        self.project_scales = ['Small', 'Medium', 'Large', 'Mega']
        self.competitive_intensities = ['Low', 'Moderate', 'High']

    def generate_tenders(self, num_records: int) -> list:
        """Generate synthetic tender data with enhanced attributes"""
        tenders = []
        for i in range(num_records):
            region = random.choice(self.regions)
            country = random.choice(self.countries[region])
            subregion = random.choice(self.subregions.get(country, [country]))
            sector = random.choice(self.sectors)
            project_scale = random.choice(self.project_scales)
            value_range = {
                'Small': (5, 50), 'Medium': (50, 200), 
                'Large': (200, 1000), 'Mega': (1000, 5000)
            }
            tender = {
                'tender_id': f"TID-{20000 + i:05d}",
                'project_name': f"{random.choice(self.project_types)} in {subregion}, {country}",
                'sector': sector,
                'region': region,
                'country': country,
                'subregion': subregion,
                'value_usd_m': round(random.uniform(*value_range[project_scale]), 2),
                'bid_deadline': fake.date_between(start_date='today', end_date='+3y'),
                'client': random.choice(self.clients),
                'project_start_date': fake.date_between(start_date='+6m', end_date='+5y'),
                'duration_months': random.randint(6, 96),
                'esg_requirements': random.choice(self.esg_focus),
                'risk_factors': random.choice(self.risk_factors),
                'procurement_type': random.choice(self.procurement_types),
                'project_complexity': random.choice(['Low', 'Medium', 'High']),
                'project_scale': project_scale,
                'description': f"Open tender for a {sector.lower()} {project_scale.lower()}-scale project: {random.choice(self.project_types).lower()} in {subregion}, {country}. Requires {random.choice(['advanced engineering', 'sustainable materials', 'local labor', 'BIM integration', 'smart city tech', 'modular construction'])}."
            }
            tenders.append(tender)
        return tenders

    def generate_competitor_activities(self, num_records: int) -> list:
        """Generate synthetic competitor activity data with impact analysis"""
        activities = []
        for i in range(num_records):
            company = random.choice(self.companies)
            region = random.choice(self.regions)
            country = random.choice(self.countries[region])
            subregion = random.choice(self.subregions.get(country, [country]))
            sector = random.choice(self.sectors)
            activity_type = random.choice([
                'Bid Submitted', 'Partnership Formed', 'Project Proposal', 
                'R&D Investment', 'Talent Acquisition', 'Marketing Campaign', 
                'ESG Certification'
            ])
            activity = {
                'activity_id': f"ACT-{20000 + i:05d}",
                'company': company,
                'activity_type': activity_type,
                'sector': sector,
                'region': region,
                'country': country,
                'subregion': subregion,
                'date': fake.date_between(start_date='-3y', end_date='today'),
                'investment_usd_m': round(random.uniform(0.2, 500), 2) if activity_type in ['R&D Investment', 'Talent Acquisition', 'Marketing Campaign'] else None,
                'partner': random.choice(self.companies) if activity_type == 'Partnership Formed' else None,
                'activity_impact': random.choice(['Low', 'Medium', 'High']),
                'competitor_size': random.choice(['Large', 'Mid-size', 'Small']),
                'details': f"{company} {activity_type.lower()} for a {sector.lower()} project in {subregion}, {country}. Focus on {random.choice(['sustainability', 'digital tools', 'cost efficiency', 'local expertise', 'client engagement', 'workforce diversity'])}."
            }
            activities.append(activity)
        return activities

    def generate_project_wins(self, num_records: int) -> list:
        """Generate synthetic project win data with contract details"""
        wins = []
        for i in range(num_records):
            company = random.choice(self.companies)
            region = random.choice(self.regions)
            country = random.choice(self.countries[region])
            subregion = random.choice(self.subregions.get(country, [country]))
            sector = random.choice(self.sectors)
            project_scale = random.choice(self.project_scales)
            value_range = {
                'Small': (10, 100), 'Medium': (100, 500), 
                'Large': (500, 2000), 'Mega': (2000, 10000)
            }
            win = {
                'win_id': f"WIN-{20000 + i:05d}",
                'company': company,
                'project_name': f"{random.choice(self.project_types)} in {subregion}, {country}",
                'sector': sector,
                'region': region,
                'country': country,
                'subregion': subregion,
                'value_usd_m': round(random.uniform(*value_range[project_scale]), 2),
                'award_date': fake.date_between(start_date='-5y', end_date='today'),
                'client': random.choice(self.clients),
                'project_duration_months': random.randint(6, 84),
                'esg_compliance': random.choice(self.esg_focus),
                'contract_type': random.choice(self.contract_types),
                'completion_status': random.choice(self.completion_status),
                'project_scale': project_scale,
                'description': f"{company} secured a {sector.lower()} {project_scale.lower()}-scale contract to build a {random.choice(self.project_types).lower()} in {subregion}, {country}. Project emphasizes {random.choice(['sustainability', 'innovation', 'community impact', 'safety', 'digitalization'])}."
            }
            wins.append(win)
        return wins

    def generate_strategic_movements(self, num_records: int) -> list:
        """Generate synthetic strategic movement data with corporate strategies"""
        movements = []
        for i in range(num_records):
            company = random.choice(self.companies)
            region = random.choice(self.regions)
            country = random.choice(self.countries[region])
            subregion = random.choice(self.subregions.get(country, [country]))
            strategy_type = random.choice(self.strategies)
            movement = {
                'movement_id': f"STR-{20000 + i:05d}",
                'company': company,
                'strategy_type': strategy_type,
                'sector_target': random.choice(self.sectors),
                'region': region,
                'country': country,
                'subregion': subregion,
                'date': fake.date_between(start_date='-5y', end_date='today'),
                'investment_usd_m': round(random.uniform(10, 15000), 2) if strategy_type in ['Merger', 'Acquisition', 'Technology Adoption', 'Market Expansion'] else None,
                'target_company': random.choice(self.companies) if strategy_type in ['Merger', 'Acquisition'] else None,
                'strategic_priority': random.choice(self.strategic_priorities),
                'details': f"{company} announced a {strategy_type.lower()} in {subregion}, {country}, targeting {random.choice(self.sectors).lower()} to {random.choice(['enhance capabilities', 'capture market share', 'drive sustainability', 'leverage digital tools', 'expand globally', 'improve resilience'])}."
            }
            movements.append(movement)
        return movements

    def generate_market_opportunities(self, num_records: int) -> list:
        """Generate synthetic market opportunity data by sector and geography"""
        opportunities = []
        for i in range(num_records):
            region = random.choice(self.regions)
            country = random.choice(self.countries[region])
            subregion = random.choice(self.subregions.get(country, [country]))
            sector = random.choice(self.sectors)
            opportunity = {
                'opportunity_id': f"OPP-{20000 + i:05d}",
                'sector': sector,
                'region': region,
                'country': country,
                'subregion': subregion,
                'market_size_usd_b': round(random.uniform(0.1, 200), 2),
                'growth_rate_percent': round(random.uniform(0.5, 30), 2),
                'investment_potential_usd_m': round(random.uniform(20, 15000), 2),
                'key_drivers': random.choice([
                    'Urbanization', 'Infrastructure Demand', 'Green Policies', 
                    'Economic Growth', 'Digitalization', 'Population Growth', 
                    'Energy Transition'
                ]),
                'key_players': ', '.join(random.sample(self.companies, random.randint(3, 10))),
                'forecast_year': random.randint(2026, 2045),
                'risk_level': random.choice(self.risk_levels),
                'regulatory_barriers': random.choice(['High', 'Moderate', 'Low', 'None']),
                'competitive_intensity': random.choice(self.competitive_intensities),
                'description': f"High-potential opportunity in {sector.lower()} sector in {subregion}, {country} ({region}), driven by {random.choice(['urban growth', 'government investment', 'sustainability goals', 'tech adoption', 'infrastructure needs', 'energy demands'])}."
            }
            opportunities.append(opportunity)
        return opportunities

    def save_to_csv_and_json(self, data: list, base_filename: str):
        """Save data to both CSV and JSON formats"""
        try:
            if not data:
                logger.warning(f"No data to save for {base_filename}")
                return
            
            # Save to CSV
            df = pd.DataFrame(data)
            csv_filename = f"{base_filename}.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            logger.info(f"Saved {len(df)} records to {csv_filename}")
            
            # Save to JSON
            json_filename = f"{base_filename}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Saved {len(data)} records to {json_filename}")
            
        except Exception as e:
            logger.error(f"Error saving {base_filename}: {e}")

def main():
    """Generate 5000+ synthetic AEC datasets and save in CSV and JSON"""
    try:
        generator = CustomAECDataGenerator()
        
        # Generate datasets
        logger.info("Generating 5000+ synthetic AEC datasets...")
        datasets = {
            'tenders': generator.generate_tenders(1400),  # 1400 records
            'competitor_activities': generator.generate_competitor_activities(1400),  # 1400 records
            'project_wins': generator.generate_project_wins(1400),  # 1400 records
            'strategic_movements': generator.generate_strategic_movements(400),  # 400 records
            'market_opportunities': generator.generate_market_opportunities(400)  # 400 records
        }
        
        # Save datasets
        for name, data in datasets.items():
            generator.save_to_csv_and_json(data, f"aec_{name}")
        
        # Print summary
        print("\n=== Custom Synthetic AEC Data Generation Summary ===")
        total_records = sum(len(data) for data in datasets.values())
        print(f"Total records generated: {total_records}")
        for name, data in datasets.items():
            print(f"{name.replace('_', ' ').title()}: {len(data)} records")
        print("\nFiles saved:")
        for name in datasets:
            print(f"- aec_{name}.csv")
            print(f"- aec_{name}.json")
        print("\nUse these datasets for strategic AEC analysis, forecasting, or visualization!")
        print("Generated on: 08:40 PM +0545, Sunday, June 01, 2025")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        print("An error occurred. Please check the logs for details.")

if __name__ == "__main__":
    main()