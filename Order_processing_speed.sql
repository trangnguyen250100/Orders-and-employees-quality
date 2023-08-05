-- Bảng chi tiết đơn thao tác muộn quá deadline

DROP TABLE IF EXISTS test.false_package; 
CREATE TABLE test.false_package STORED AS PARQUET AS 
SELECT ngay, id_chi_nhanh, ma_don, tgian_nhap_chi_nhanh, tgian_thao_tac, deadline,
            ROW_NUMBER() OVER (PARTITION BY ngay, id_chi_nhanh, ma_don, tg_nhap_chi_nhanh, tg_thao_tac ORDER BY deadline) stt_deadline
FROM (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY ngay, branch_id, , ma_don, import_time, deadline ORDER BY tgian_bat_dau) rn 
    FROM (
        SELECT  t.ngay, t.branch_id, t.ma_don, tgian_nhap_chi_nhanh, tgian_thao_tac,
            CASE 
                WHEN d.ngay =  CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT) 
                    AND (CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'HHmm') AS INT) BETWEEN cf.tgian_bat_dau AND cf.tgian_ket_thuc) then t.deadline
                ELSE MINUTES_ADD(HOURS_ADD(TRUNC(cf.ngay,'DD'), cf.gio_deadline), cf.phut_deadline)
            END deadline
        FROM test.all_don t
        LEFT JOIN test.deadline d 
                ON t.branch_id = cf.branch_id 
                AND 
                    (
                        ((CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'HHmm') AS INT) BETWEEN d.tgian_bat_dau AND cf.tgian_ket_thuc)  
                        AND cf.ngay = CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT))
                    OR 
                        (
                            (
                                ((CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'HHmm') AS INT) < cf.tgian_ket_thuc)
                                AND cf.ngay = CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT)) 
                                OR  cf.ngay > CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT)
                            )
                        AND MINUTES_ADD(HOURS_ADD(TRUNC(cf.ngay,'DD'), cf.gio_deadline), cf.phut_deadline) < tgian_thao_tac
                        )
                    )
        ) t
    ) t 
where rn = 1 
; 

-- Bảng gán người bị phạt với đơn sai 
DROP TABLE IF EXISTS test.red_card; 
CREATE TABLE test.red_card STORED AS PARQUET AS 
SELECT  p.*, em.ma_nhan_vien
    CASE
        WHEN p.deadline is null then null
        WHEN stt_deadline = 1 then 
            CASE 
                WHEN em.tgian_thao_tac_truoc_deadline is not null AND em.tgian_thao_tac_sau_deadline is not null then 1 
                WHEN em.tgian_thao_tac_sau_deadline is null AND em.tgian_thao_tac_truoc_deadline BETWEEN tgian_thao_tac AND p.deadline then 1 
                ELSE 0 
            END 
        WHEN stt_deadline > 1 AND em.tgian_thao_tac_truoc_deadline is not null then 1 
        ELSE 0 
    END bi_the_do
FROM test.false_package p 
LEFT JOIN test.employee em
        ON p.branch_id = em.branch_id 
        AND p.deadline >= u.pre_node AND p.deadline < u.next_node
        AND em.ngay_lam_viec = p.ngay
;
