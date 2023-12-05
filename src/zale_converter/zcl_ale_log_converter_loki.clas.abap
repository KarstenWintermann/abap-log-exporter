CLASS zcl_ale_log_converter_loki DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    INTERFACES zif_ale_log_converter.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS ZCL_ALE_LOG_CONVERTER_LOKI IMPLEMENTATION.


  METHOD zif_ale_log_converter~convert.

    LOOP AT logs ASSIGNING FIELD-SYMBOL(<log>).

      DATA(object) = <log>-additional_fields[ field = 'object' ]-value.

      DATA(utclong) = cl_abap_tstmp=>tstmp2utclong(
        EXPORTING
          timestamp = <log>-timestamp
      ).

      CONVERT UTCLONG utclong TIME ZONE 'UTC'
        INTO DATE DATA(dat) TIME DATA(tim) FRACTIONAL SECONDS DATA(fs).

      DATA msec TYPE num03.
      msec = fs * 1000.

      cl_pco_utility=>convert_abap_timestamp_to_java(
        EXPORTING
          iv_date      = dat                 " Date Data Element for SYST
          iv_time      = tim                 " Date and Time, Current Application Server Time
          iv_msec      = msec                " Remaining Milliseconds
        IMPORTING
          ev_timestamp = DATA(ts)                 " Time in Milliseconds since 1.1.1970, 00:00:00 GMT
      ).

      DATA(loki_json) = |\{"streams": | &&
                        |[| &&
                        |\{ "stream": \{ "object": "{ object }" \},| &&
                        |"values": [| &&
                        |["{ ts }000000", "{ <log>-item_text }" ]| &&
                        |] \}]\}|.

      converted_logs = VALUE #( BASE converted_logs ( json = loki_json ) ).

    ENDLOOP.
  ENDMETHOD.
ENDCLASS.
