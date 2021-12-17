truncate table dim_case;

insert into dim_case
with ds as (
    select distinct
           case
               when (suspect > 0 or suspect_diisolasi > 0 or suspect_discarded > 0 or
                     suspect_meninggal > 0) then 'SUSPECT'
               when (closecontact > 0 or closecontact_dikarantina > 0 or
                     closecontact_discarded > 0 or closecontact_meninggal > 0) then 'CLOSECONTACT'
               when (probable > 0 or probable_diisolasi > 0 or probable_discarded > 0 or
                     probable_meninggal > 0) then 'PROBABLE'
               when (confirmation > 0 or confirmation_meninggal > 0 or confirmation_sembuh > 0)
                   then 'CONFIRMATION' else 'OTHERS' end as status_name,
           case
               when suspect > 0 then 'SUSPECT'
               when suspect_meninggal > 0 then 'SUSPECT_MENINGGAL'
               when suspect_discarded > 0 then 'SUSPECT_DISCARDED'
               when suspect_diisolasi > 0 then 'SUSPECT_DIISOLASI'
               when closecontact > 0 then 'CLOSECONTACT'
               when closecontact_dikarantina > 0 then 'CLOSECONTACT_DIKARANTINA'
               when closecontact_discarded > 0 then 'CLOSECONTACT_DISCARDED'
               when closecontact_meninggal > 0 then 'CLOSECONTACT_MENINGGAL'
               when probable > 0 then 'PROBABLE'
               when probable_diisolasi > 0 then 'PROBABLE_DIISOLASI'
               when probable_discarded > 0 then 'PROBABLE_DISCARDED'
               when probable_meninggal > 0 then 'PROBABLE_MENINGGAL'
               when confirmation > 0 then 'CONFIRMATION'
               when confirmation_sembuh > 0 then 'CONFIRMATION_SEMBUH'
               when confirmation_meninggal > 0 then 'CONFIRMATION_MENINGGAL'
            else 'OTHERS'
        end                         as status_detail
    from stg_covid_data
) select row_number() over (order by status_name desc) as status_id, *
from ds
where status_name is not null;
