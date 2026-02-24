function transform(input) {
  const vendor = input.trmnl.plugin_settings.custom_fields_values.vendor || "rebrickable";
  const minYear = parseInt(input.trmnl.plugin_settings.custom_fields_values.min_year) || 0;
  const maxYear = parseInt(input.trmnl.plugin_settings.custom_fields_values.max_year) || 0;
  const minParts = parseInt(input.trmnl.plugin_settings.custom_fields_values.min_parts) || 0;
  const maxParts = parseInt(input.trmnl.plugin_settings.custom_fields_values.max_parts) || 0;
  const showBrickCount = input.trmnl.plugin_settings.custom_fields_values.show_brick_count || "yes";
  const showMinifigs = input.trmnl.plugin_settings.custom_fields_values.brickset_show_minifigs_included || "no";
  const showOwnedWanted = input.trmnl.plugin_settings.custom_fields_values.brickset_show_owned_wanted_indicators || "yes";
  const pricingRegion = input.trmnl.plugin_settings.custom_fields_values.brickset_pricing || "";
  const showQrCode = input.trmnl.plugin_settings.custom_fields_values.show_qr_code || "hide";
  
  let allSets = [];
  const bricksetMode = vendor === "brickset";
  
  // Normalize data based on vendor
  if (bricksetMode) {
    // Brickset data - sets are at root level, not in input.data
    const rawSets = input.sets;  // Changed from input.data?.sets
    
    if (!rawSets || !Array.isArray(rawSets) || rawSets.length === 0) {
      return {
        error: "No data available",
        sets_count: 0,
        brickset_mode: true,
        min_year: minYear > 0 ? minYear : null,
        max_year: maxYear > 0 ? maxYear : null,
        min_parts: minParts > 0 ? minParts : null,
        max_parts: maxParts > 0 ? maxParts : null,
        input: input
      };
    }
    // Normalize Brickset data
    allSets = rawSets.map(s => {
      const setNum = s.number || "";
      const name = s.name || "";
      const year = parseInt(s.year) || 0;
      const numParts = parseInt(s.pieces) || 0;
      const theme = s.theme || "";
      const subtheme = s.subtheme || "";
      const image = s.image?.imageURL || "https://via.placeholder.com/300x300?text=No+Image";
      const minifigs = parseInt(s.minifigs) || 0;
      const owned = s.collection?.owned || false;
      const wanted = s.collection?.wanted || false;
      
      // Price formatting
      let price = "";
      if (pricingRegion && s.LEGOCom && s.LEGOCom[pricingRegion] && s.LEGOCom[pricingRegion].retailPrice) {
        const priceValue = parseFloat(s.LEGOCom[pricingRegion].retailPrice).toFixed(2);
        let currency = "";
        if (pricingRegion === "US" || pricingRegion === "CA") {
          currency = "$";
        } else if (pricingRegion === "UK") {
          currency = "£";
        } else if (pricingRegion === "DE") {
          currency = "€";
        }
        price = `${currency} ${priceValue}`;
      }
      
      return {
        set_num: setNum,
        name,
        year,
        num_parts: numParts,
        theme,
        parent_theme: subtheme,
        image,
        minifigs,
        owned: owned ? "1" : "0",
        wanted: wanted ? "1" : "0",
        price
      };
    });
    
  } else {
    // Rebrickable data
    const raw = input.data;
    if (!raw || !Array.isArray(raw) || raw.length === 0) {
      return {
        error: "No data available",
        sets_count: 0,
        brickset_mode: false,
        min_year: minYear > 0 ? minYear : null,
        max_year: maxYear > 0 ? maxYear : null,
        min_parts: minParts > 0 ? minParts : null,
        max_parts: maxParts > 0 ? maxParts : null
      };
    }
    
    // Skip header row
    const dataSets = raw.slice(1);
    
    if (dataSets.length === 0) {
      return {
        error: "No data available",
        sets_count: 0,
        brickset_mode: false,
        min_year: minYear > 0 ? minYear : null,
        max_year: maxYear > 0 ? maxYear : null,
        min_parts: minParts > 0 ? minParts : null,
        max_parts: maxParts > 0 ? maxParts : null,
        input: input
      };
    }
    
    allSets = dataSets.map(row => {
      const setNum = row[0] || "";
      const name = row[1] || "";
      const year = parseInt(row[2]) || 0;
      const numParts = parseInt(row[3]) || 0;
      const theme = row[4] || "";
      const subtheme = row[5] || "";
      const image = `https://cdn.rebrickable.com/media/sets/${setNum}.jpg`;
      
      return {
        set_num: setNum,
        name,
        year,
        num_parts: numParts,
        theme,
        parent_theme: subtheme,
        image,
        minifigs: 0,
        owned: "0",
        wanted: "0",
        price: ""
      };
    });
  }
  
  // Apply filters
  const filteredSets = allSets.filter(set => {
    if (minYear > 0 && set.year < minYear) return false;
    if (maxYear > 0 && set.year > maxYear) return false;
    if (minParts > 0 && set.num_parts < minParts) return false;
    if (maxParts > 0 && set.num_parts > maxParts) return false;
    return true;
  });
  
  if (filteredSets.length === 0) {
    return {
      error: "No sets match your filters",
      sets_count: 0,
      brickset_mode: bricksetMode,
      min_year: minYear > 0 ? minYear : null,
      max_year: maxYear > 0 ? maxYear : null,
      min_parts: minParts > 0 ? minParts : null,
      max_parts: maxParts > 0 ? maxParts : null
    };
  }
  
  // Select random set
  const now = Date.now();
  const selectedIndex = now % filteredSets.length;
  const selectedSet = filteredSets[selectedIndex];
  
  // Apply show_brick_count setting
  if (showBrickCount !== "yes") {
    selectedSet.num_parts = 0;
  }
  
  // Apply show minifigs setting
  if (showMinifigs !== "yes") {
    selectedSet.minifigs = 0;
  }
  
  // Apply owned/wanted indicators setting
  if (showOwnedWanted !== "yes") {
    selectedSet.owned = "0";
    selectedSet.wanted = "0";
  }
  
  // Generate URLs
  const rebrickable_url = `https://rebrickable.com/sets/${selectedSet.set_num}`;
  const brickset_url = `https://brickset.com/sets/${selectedSet.set_num}`;
  
  // Determine if info panel should be shown
  const show_infopanel = 
    (selectedSet.num_parts > 0) ||
    (selectedSet.minifigs > 0) ||
    (selectedSet.owned === "1") ||
    (selectedSet.wanted === "1") ||
    (selectedSet.price !== "");
  
  return {
    set_num: selectedSet.set_num,
    name: selectedSet.name,
    year: selectedSet.year,
    num_parts: selectedSet.num_parts,
    theme: selectedSet.theme,
    parent_theme: selectedSet.parent_theme,
    image: selectedSet.image,
    minifigs: selectedSet.minifigs,
    owned: selectedSet.owned,
    wanted: selectedSet.wanted,
    price: selectedSet.price,
    rebrickable_url,
    brickset_url,
    show_infopanel,
    show_qr_code: showQrCode,
    sets_count: filteredSets.length,
    vendor,
    brickset_mode: bricksetMode,
    brickset_show_minifigs_included: showMinifigs,
    brickset_show_owned_wanted_indicators: showOwnedWanted,
    min_year: minYear > 0 ? minYear : null,
    max_year: maxYear > 0 ? maxYear : null,
    min_parts: minParts > 0 ? minParts : null,
    max_parts: maxParts > 0 ? maxParts : null
  };
}