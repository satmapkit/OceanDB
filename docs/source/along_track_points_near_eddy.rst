Using along_track_points_near_eddy
==================================

:class:`Eddy.along_track_points_near_eddy <OceanDB.data_access.eddy.Eddy.along_track_points_near_eddy>` returns along-track altimetry points that
are associated with one eddy track.

Basic Usage
-----------

.. code-block:: python

   from OceanDB.data_access.eddy import Eddy

   eddy = Eddy()
   result = eddy.along_track_points_near_eddy(track_id=-1)

If data is found, the method returns a
:class:`Dataset <OceanDB.ocean_data.dataset.Dataset>`.
If no along-track points match the query, it returns ``None``.
If the eddy track does not exist, it raises ``ValueError``.

Selecting Fields
----------------

By default, the method returns a standard set of along-track fields, including
position, time, track metadata, and several sea level anomaly corrections.

.. code-block:: python

   from OceanDB.data_access.eddy import Eddy

   eddy = Eddy()
   result = eddy.along_track_points_near_eddy(
       track_id=-1,
       fields=["latitude", "longitude", "date_time", "sla_filtered"],
   )

The allowed fields are:
``latitude`` ``longitude`` ``date_time`` ``file_name`` ``mission`` ``track``
``cycle`` ``basin_id`` ``sla_unfiltered`` ``sla_filtered`` ``dac`` ``ocean_tide``
``internal_tide`` ``lwe`` ``mdt`` ``tpa_correction`` ``distance`` and ``delta_t``.

See :meth:`API reference for Eddy.along_track_points_near_eddy <OceanDB.data_access.eddy.Eddy.along_track_points_near_eddy>` for more info.


Track IDs
---------

The method expects the signed OceanDB eddy track id:

- negative values represent cyclonic eddies
- positive values represent anticyclonic eddies

For example, the integration test uses ``track_id=-1``.

Example Workflow
----------------

.. code-block:: python

   from OceanDB.data_access.eddy import Eddy

   eddy = Eddy()
   data = eddy.along_track_points_near_eddy(
       track_id=-1,
       fields=["latitude", "longitude", "date_time", "sla_filtered", "track"],
   )

   if data is None:
       print("No along-track points found for this eddy")
   else:
       print(data["date_time"])
       print(data["sla_filtered"])
