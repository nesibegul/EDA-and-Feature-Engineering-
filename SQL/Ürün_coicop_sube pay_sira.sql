select * 
    from (
        select  
            s1.yil|| s1.ay as yil_ay,
            s1.vergino,
            s3.isyeri_tanim as isyeri,
            s1.sube,
            s1.bolge_kodu,
            s1.il_kodu,
            s1.madde_kod,
            s2.aciklama  as madde_tanim,
           -- s1.barkod,
            s1.marka,
            s1.tanim,
           -- s1.il_ciro,
           -- s1.sube_sira, 
           -- s1.sube_pay,
           -- s1.sube_ciro,
            s1.coicop_sira,
            s1.coicop_pay,
          --  s1.coicop_ciro,
            s1.tanim_sira,
            s1.tanim_pay
          --  s1.tanim_ciro
        from(      
         select 
                yil,
                ay,
                vergino,
                sube,
                bolge_kodu,
                il_kodu,
                madde_kod,
              --  barkod,
                marka,
                tanim,
               -- il_ciro,
                --dense_rank() over(partition by yil, ay, vergino, bolge_kodu,il_ciro order by sube_ciro desc nulls last) as sube_sira,
               -- round(sube_ciro/il_ciro*100, 2) as sube_pay,
               -- sube_ciro,
                dense_rank() over(partition by yil, ay, sube order by coicop_ciro desc nulls last) as coicop_sira,
                round(coicop_ciro/ sube_ciro*100,2) as coicop_pay,
                coicop_ciro,
                dense_rank() over(partition by yil, ay, sube,coicop_ciro order by tanim_ciro desc nulls last) as tanim_sira,
                round(tanim_ciro/ coicop_ciro*100,2) as tanim_pay,
                tanim_ciro        
            from(         
                select /* +parallel(b,6)*/
                    yil,
                    ay,
                    vergino,
                    sube,
                    bolge_kodu,
                    il_kodu,
                    madde_kod,
                  --  barkod,
                    marka,
                    tanim,
                    --sum(tanim_ciro) over(partition by yil, ay, vergino, bolge_kodu,il_kodu) as il_ciro,    
                    sum(tanim_ciro) over(partition by yil, ay, sube) as sube_ciro,
                    sum(tanim_ciro) over(partition by yil, ay, sube, madde_kod) as coicop_ciro,
                    tanim_ciro
                from(
                    select /* +parallel(a,6)*/
                        yil,
                        ay,
                        vergino,
                        sube,
                        bolge_kodu,
                        il_kodu,
                        madde_kod,
                      --  barkod,
                        marka,
                        tanim,
                        sum(satis_ciro) as tanim_ciro       
                    from E_006_BARKOD_URUN_MIKTAR_PAR a
                        where yil= 2021
                            and ay <> 1
                            and vergino = (select vergino from ref_barkod_isyeri where sira = 3)
                    group by
                        yil,
                        ay,
                        vergino,
                        sube,
                        bolge_kodu,
                        il_kodu,
                        madde_kod,
                       -- barkod,
                        marka,
                        tanim   
                                
               )b) 
                where madde_kod is not null
                      and madde_kod <> 1
                           

            ) s1 
            left join
            (
            
                select
                     kod, 
                     aciklama
                from ref_barkod_coicop 
                    where seviye = 11   
             ) s2
             on s1.madde_kod = s2.kod
             
           left join
           
           ( 
            select 
                vergino,
                isyeri_tanim
            from ref_barkod_isyeri
            ) s3
            on s1.vergino = s3.vergino
    ) 
        pivot(
                max (coicop_sira) as coicop_sira,
                max (coicop_pay) as coicop_pay,
               -- max (coicop_ciro) as coicop_ciro,
                max (tanim_sira) as tanim_sira,
                max (tanim_pay) as tanim_pay
              --  max (tanim_ciro) as tanim_ciro
                
        for yil_ay in
            (
                20212 as y20212,
                20213 as y20213,
                20214 as y20214,
                20215 as y20215,
                20216 as y20216,
                20217 as y20217,
                20218 as y20218,
                20219 as y20219,
                202110 as y202110,
                202111 as y202111
                
                
             )
             )