listObjects : will get json files of prefix (not include the folder)
const bucketName='household' 
const path= "household-HCMC-Q1-0/2025-04-13/6"
{
  name: 'household-HCMC-Q1-0/2025-04-13/6.json',
  lastModified: 2025-04-13T08:12:40.104Z,
  etag: '0cb5b88204228131f98102e8c5adbbd8',
  size: 479
}
{
  name: 'household-HCMC-Q1-0/2025-04-13/6/25.json',
  lastModified: 2025-04-13T08:12:39.009Z,
  etag: '0cb5b88204228131f98102e8c5adbbd8',
  size: 479
}


getObject + buffer + totSring
{"type": "HouseholdData", "household_id": "household-HCMC-Q1-0", "device_id": "household-HCMC-Q1-0", "timestamp": "2025-04-13T06:25:30.698000", "electricity_usage_kwh": 195.0399932861328, "voltage": 230, "current": 23.760000228881836, "location": ["265 Vaughn Cove", "Phuong 10", "Quan 1", "Ho Chi Minh City"], "price_per_kwh": 2618, "total_cost": 510614, "year": 2025, "month": 4, "hour": 6, "minute": 25, "date_part": "2025-04-13", "formatted_timestamp": "2025-04-13 06-25-30"}