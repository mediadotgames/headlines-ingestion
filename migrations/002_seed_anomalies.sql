-- initdb/002_seed_anomalies.sql
-- --------------------------------------------------------------------
-- Manually seed known anomalies for local testing
-- --------------------------------------------------------------------

INSERT INTO anomalies
  (summary, report_time, category, severity, reporter, tags, outlets,
   resolution_status, investigator, added_context)
VALUES
  (
    'New Source Performance Standards for Greenhouse Gas Emissions from New and Reconstructed Fossil Fuel-Fired Electric Generating Units; Emissions Guidelines for Greenhouse Gas Emissions from Existing Fossil Fuel-Fired Electric Generating Units; and Repeal of the Affordable Clean Energy Rule',
    '2025-10-14T15:45:00-04:00',
    'Ignored Policy Proposals',
    'High',
    'Jesse M',
    ARRAY['EPA', 'Policy'],
    ARRAY['Outlet A', 'Outlet B'],
    'Pending',
    'Paul R',
    'Arbitrary Context'
  ),
  (
    'This proposed rule would set forth routine updates to the Medicare home health payment rates in accordance with existing statutory and regulatory requirements. It proposes permanent and temporary behavior adjustments and proposes to recalibrate the case-mix weights and update the functional impairment levels; comorbidity subgroups; and low-utilization payment adjustment thresholds for CY 2026. In addition, it proposes policy changes to the face-to-face encounter policy. It also proposes changes to the Home Health Quality Reporting Program and the expanded Health Value-Based Purchasing Model requirements. Lastly, it proposes to update the Durable Medical Equipment, Prosthetics, Orthotics, and Supplies (DMEPOS) Competitive Bidding Program; make a technical change to the HH conditions of participation; update the DMEPOS supplier conditions of payment; update the provider and supplier enrollment requirements; and make changes to DMEPOS accreditation requirements.',
    '2025-10-13T13:45:00-04:00',
    'Ignored Policy Proposals',
    'Medium',
    'Paul R',
    ARRAY['CMS', 'Policy'],
    ARRAY['Outlet B', 'Outlet C'],
    'Validated',
    'Jesse M',
    'Arbitrary context'
  );
