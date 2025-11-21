CREATE TABLE IF NOT EXISTS Dim_Staff (
    Staff_Key SERIAL PRIMARY KEY,        
    staff_id VARCHAR,                
    staff_name VARCHAR,             
    staff_job_level VARCHAR,        
    staff_street VARCHAR,           
    staff_city VARCHAR,             
    staff_state VARCHAR,            
    staff_country VARCHAR          
);

INSERT INTO Dim_Staff (
    staff_id, 
    staff_name, 
    staff_job_level, 
    staff_street, 
    staff_city, 
    staff_state, 
    staff_country
)

SELECT DISTINCT 
    staff_id, 
    name,          
    job_level,    
    street,         
    city,          
    state,         
    country         
FROM staging_staff_data;