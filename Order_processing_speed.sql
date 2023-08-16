-- Bảng chi tiết đơn hàng thao tác quá deadline 
-- Bảng bao gồm: ngày xử lý đơn, id chi nhánh xử lý đơn, mã đơn, thời gian đơn hàng nhập vào chi nhánh, thời gian xử lý đơn hàng, deadline xử lý đơn hàng, và số thứ tự deadline xử lý đơn hàng cho đến khi đơn hàng được xử lý
DROP TABLE IF EXISTS test.false_package; 
CREATE TABLE test.false_package STORED AS PARQUET AS 
SELECT ngay, id_chi_nhanh, ma_don, tgian_nhap_chi_nhanh, tgian_thao_tac, deadline,
            ROW_NUMBER() OVER (PARTITION BY ngay, id_chi_nhanh, ma_don, tg_nhap_chi_nhanh, tg_thao_tac ORDER BY deadline) stt_deadline -- sắp xếp số thứ tự deadline
FROM (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY ngay, id_chi_nhanh, ma_don, tgian_nhap_chi_nhanh, deadline ORDER BY tgian_bat_dau) rn -- loại bỏ dữ liệu trùng lặp khi có hơn 1 thời gian bắt đầu
    FROM (
        SELECT  t.ngay, t.id_chi_nhanh, t.ma_don, tgian_nhap_chi_nhanh, tgian_thao_tac,
            CASE 
                WHEN d.ngay =  CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT) 
                    AND (CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'HHmm') AS INT) BETWEEN d.tgian_bat_dau AND d.tgian_ket_thuc) then t.deadline
                ELSE MINUTES_ADD(HOURS_ADD(TRUNC(d.ngay,'DD'), d.gio_deadline), d.phut_deadline)
            END deadline
        FROM test.all_don t
        LEFT JOIN test.deadline d 
                ON t.id_chi_nhanh = cf.id_chi_nhanh 
                AND 
                    (
                        -- lấy mốc deadline đầu tiên
                        ((CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'HHmm') AS INT) BETWEEN d.tgian_bat_dau AND d.tgian_ket_thuc)  
                        AND d.ngay = CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT))
                    OR
                        -- lấy các mốc deadline tiếp theo cho đến khi đơn được xử lý
                        (  
                            (
                                ((CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'HHmm') AS INT) < d.tgian_ket_thuc)
                                AND d.ngay = CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT)) 
                                OR  d.ngay > CAST(FROM_timestamp(t.tgian_nhap_chi_nhanh, 'yyyyMMdd') AS INT)
                            )
                        AND MINUTES_ADD(HOURS_ADD(TRUNC(d.ngay,'DD'), d.gio_deadline), d.phut_deadline) < tgian_thao_tac
                        )
                    )
        ) t
    ) t 
where rn = 1 -- loại bỏ dữ liệu trùng lặp
; 

-- Bảng gán người bị phạt với đơn sai, những người này có thời gian xử lý đơn hàng trước/sau mốc deadline đơn bị sai nhưng không xử lý đơn sai thì bị gán phạt
DROP TABLE IF EXISTS test.red_card; 
CREATE TABLE test.red_card STORED AS PARQUET AS 
SELECT  p.*, em.ma_nhan_vien
    CASE
        WHEN p.deadline is null then null 
        WHEN stt_deadline = 1 then 
            CASE 
                WHEN em.tgian_thao_tac_truoc_deadline is not null AND em.tgian_thao_tac_sau_deadline is not null then 1 
                WHEN em.tgian_thao_tac_sau_deadline is null AND em.tgian_thao_tac_truoc_deadline BETWEEN tg_nhap_chi_nhanh AND p.deadline then 1
            END 
        WHEN stt_deadline > 1 AND em.tgian_thao_tac_truoc_deadline is not null then 1 
    END bi_the_do -- 1 là bị thẻ, 0 là không bị thẻ
FROM test.false_package p 
LEFT JOIN test.employees em
        ON p.id_chi_nhanh = em.id_chi_nhanh 
        AND p.deadline >= u.deadline_truoc AND p.deadline < u.deadline_sau
        AND em.ngay_lam_viec = p.ngay
;
