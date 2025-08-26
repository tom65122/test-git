CREATE TABLE IF NOT EXISTS fact_log
(
    id         INT AUTO_INCREMENT PRIMARY KEY,
    user_id    INT         NOT NULL,
    device_id  VARCHAR(50) NOT NULL,
    login_date DATE        NOT NULL,
    INDEX idx_user_login (user_id, login_date)
);

INSERT INTO fact_log (user_id, device_id, login_date)
VALUES (101, 'D001', '2024-05-01'),
       (101, 'D002', '2024-05-02'),
       (101, 'D001', '2024-05-03'),
       (102, 'D003', '2024-05-02'),
       (102, 'D003', '2024-05-04'),
       (103, 'D004', '2024-05-01'),
       (103, 'D005', '2024-05-01'),
       (103, 'D004', '2024-05-05'),
       (104, 'D006', '2024-05-03'),
       (105, 'D007', '2024-05-04');


-- 计算每个用户的首次登录日期
CREATE TEMPORARY TABLE IF NOT EXISTS user_first_login
(
    user_id          INT PRIMARY KEY,
    first_login_date DATE NOT NULL
);

INSERT INTO user_first_login (user_id, first_login_date)
SELECT user_id,
       MIN(login_date) AS first_login_date
FROM fact_log
GROUP BY user_id;


-- 统计每日新老用户数
SELECT t.login_date,
       COUNT(DISTINCT CASE WHEN t.login_date = u.first_login_date THEN t.user_id END) AS new_user_count,
       COUNT(DISTINCT CASE WHEN t.login_date > u.first_login_date THEN t.user_id END) AS old_user_count
FROM fact_log t
         LEFT JOIN user_first_login u ON t.user_id = u.user_id
GROUP BY t.login_date
ORDER BY t.login_date;