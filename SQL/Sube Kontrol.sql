select 
    vergino,
    isyeri,
    sube,
    yil,
    ay,
    kurum_key,
    bolge_kodu,
    il_kodu
from(
    select 
        s1.vergýno,
        s3.isyeri,  
        s1.sube,
        s2.yil,
        s2.ay,
        s2.kurum_key,
        s2.bolge_kodu,
        s2.il_kodu
     
    from(       
        select vergino, sube from C_005_SUBE_PAR
            where yil = 2021
                   and ay = 12
                   and vergino = (select vergino from ref_barkod_isyeri where sira = 7)
                minus
        select vergino, sube from C_005_SUBE_PAR
            where yil = 2021
                  and ay = 11
                  and vergino = (select vergino from ref_barkod_isyeri where sira = 7)
        ) s1

    left join
        (

        select
            yil,
            ay,
            vergino,
            magaza_kodu as sube,
            kurum_key,
            bolge_kodu,
            il_kodu 
        from ref_barkod_magaza
            where yil = 2021
                  and ay = 12
                  --and vergino = (select vergino from ref_barkod_isyeri where sira = 7)
        )s2
        on s1.vergino = s2.vergino 
           and s1.sube = s2.sube
        
    left join
        (select vergino, isyeri_tanim as isyeri from ref_barkod_isyeri) s3  
          
        on s1.vergino = s3.vergino
        
    )
    where il_kodu is null      
