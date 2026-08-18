-- Get SUMMARY API

SELECT
    COUNT(id) AS total_employees,
    COUNT(id) FILTER (
        WHERE prediction = 'Yes'
    ) AS predicted_attrition,
    COUNT(id) FILTER (
        WHERE prediction = 'No'
    ) AS predicted_retention,
    COUNT(id) FILTER (
        WHERE risk_level = 'HIGH'
    ) AS high_risk,
    COUNT(id) FILTER (
        WHERE risk_level = 'MEDIUM'
    ) AS medium_risk,
    COUNT(id) FILTER (
        WHERE risk_level = 'LOW'
    ) AS low_risk
FROM employee_predictions
WHERE model = 'xgboost';

-- Get Risk Level Attrition

SELECT
    risk_level,
    COUNT(id) AS total
FROM employee_predictions
WHERE model = 'xgboost'
GROUP BY risk_level
ORDER BY risk_level ASC;

-- Get Department Risk Attrition

SELECT
    e.department,
    COUNT(ep.id) AS total_employees,
    COUNT(ep.id)
        FILTER (
            WHERE ep.prediction = 'Yes'
        ) AS predicted_attrition,
    COUNT(ep.id)
        FILTER (
            WHERE ep.risk_level = 'HIGH'
        ) AS high_risk
FROM employee e
JOIN employee_predictions ep
    ON e.employee_number = ep.employee_number
WHERE ep.model = 'xgboost'
GROUP BY e.department
ORDER BY COUNT(ep.id) DESC;