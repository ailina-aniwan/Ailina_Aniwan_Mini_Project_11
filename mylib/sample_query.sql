SELECT 
    Stress_Level, 
    AVG(GPA) AS Avg_GPA, 
    AVG(Social_Hours_Per_Day) AS Avg_Social_Hours
FROM 
    student_lifestyle_delta
GROUP BY 
    Stress_Level
ORDER BY 
    Avg_GPA DESC;
