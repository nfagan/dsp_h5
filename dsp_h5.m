classdef dsp_h5 < h5_api
  
  properties;
  end;
  
  methods
    
    function obj = dsp_h5(varargin)
      obj@h5_api( varargin{:} );
    end
    
    function write_(obj, container, gname, start)
      
      %   WRITE_ -- Private method for writing Container objects to a .h5
      %     file. Overloaded to allow saving of SignalContainers.
      %
      %     A new dataset will be created if it does not already exist.
      %
      %     IN:
      %       - `container` (Container) -- Container object to save.
      %       - `gname` (char) -- Group in which to save.
      %       - `start` (double) -- Numeric index specifying the row at
      %         which to start writing data.
      
      write_@h5_api( obj, container, gname, start );
      
      switch ( class(container) )
        case 'Container'
          return;
        case 'SignalContainer'
          obj.write_signal_container( container, gname, start );          
        otherwise
          error( 'Cannot write Containers of subclass ''%s''', class(container) );
      end      
    end
    
    function write_signal_container(obj, container, gname, start)
      
      %   WRITE_SIGNAL_CONTAINER -- Write additional SignalContainer
      %     properties, after writing data and labels.
      %
      %     IN:
      %       - `gname` (char) -- Path to the group to which to save.
      
      gname = obj.ensure_leading_backslash( gname );
      data_set_path = [gname, '/data'];
      atts = { 'fs', 'start', 'stop', 'window_size', 'step_size', 'params', 'frequencies' };
      s = struct();
      for i = 1:numel(atts)
        current = container.(atts{i});
        assert( ndims(current) == 2, ['Cannot save an attribute with' ...
          , ' more than two dimensions.'] );
        assert( size(current, 2) == 1, ['Cannot save attributes that' ...
          , ' are matrices.'] );
        s.(atts{i}) = current(:)';
      end
      if ( obj.is_attr( data_set_path, 'props' ) )
        current_props = json( 'parse', obj.readatt(data_set_path, 'props') );
        if ( ~isequal(current_props, s) )
          fprintf( ['\n WARNING: The incoming object has properties that' ...
            , ' are not identical to those of the saved object(s). This' ...
            , ' is likely just due to floating point errors in the' ...
            , ' property values, but is worth checking out if you believe' ...
            , ' the properties should be truly identical.'] );
        end
      end
      s = json( 'encode', s );
      h5writeatt( obj.h5_file, data_set_path, 'props', s );
      trial_stats = container.trial_stats;
      assert( ~isfield(trial_stats, 'trial_ids') ...
        , ['Cannot save a SignalContainer whose trial_stats have a' ...
        , ' trial_ids field.'] );
      trial_stats.trial_ids = container.trial_ids;
      addtl_fields = fieldnames( trial_stats );
      current_sets = setdiff( obj.get_set_names(gname), {'data', 'labels'} );
      sets_to_check = unique( [addtl_fields; current_sets(:)] );
      for i = 1:numel(sets_to_check)
        current_set_path = [ gname, '/' sets_to_check{i} ];
        if ( ~any(strcmp(addtl_fields, sets_to_check{i})) )
          prop = zeros( size(container.data, 1), 1 );
        else
          prop = trial_stats.(sets_to_check{i});
          if ( isempty(prop) ), prop = zeros( size(container.data, 1), 1 ); end
        end
        [data_sz, data_chunk] = get_sz_chunk( prop );
        if ( ~obj.is_set(current_set_path) )
          h5create( obj.h5_file, current_set_path, data_sz, 'ChunkSize', data_chunk );  
          next_row = size( prop, 1 ) + 1;
        else
          current_row = h5readatt( obj.h5_file, current_set_path, 'next_row' );
          next_row = current_row + size( prop, 1 );
        end
        [prop_start, count] = get_start_count( prop, start );
        h5write( obj.h5_file, current_set_path, prop, prop_start, count );
        h5writeatt( obj.h5_file, current_set_path, 'next_row', next_row );
      end
      function [start, count] = get_start_count( mat, start )
        dims = ndims( mat );
        start = [ start, ones(1, dims-1) ];
        count = size( mat );
      end
      function [sz, chunk] = get_sz_chunk( mat )        
        sz = size( mat );
        dims = numel( sz );
        sz(1) = Inf;
        chunk = obj.CHUNK_SIZE( 1:dims );
        chunk = min( [chunk; sz] );
      end 
    end
    
    function cont = read(obj, gname)
      
      %   READ -- Load a Container from the given group.
      %
      %     IN:
      %       - `gname` (char) -- Path to the group housing /data and
      %         /labels datasets.
      
      obj.assert__is_group( gname );
      gname = obj.ensure_leading_backslash( gname );
      data_set_path = [ gname, '/data' ];
      obj.assert__is_set( data_set_path );
      labels = obj.read_labels_( gname );
      
      data = h5read( obj.h5_file, data_set_path );
      kind = obj.readatt( data_set_path, 'class' );
      
      switch ( kind )
        case 'Container'
          cont = Container( data, labels );
        case 'SignalContainer'
          cont = SignalContainer( data, labels );
          props = json( 'parse', obj.readatt(data_set_path, 'props') );
          addtl = setdiff( obj.get_set_names(gname), {'data', 'labels'} );
          trial_stats = struct();
          for i = 1:numel(addtl)
            current_set = [ gname, '/', addtl{i} ];
            trial_stats.(addtl{i}) = h5read( obj.h5_file, current_set );
          end
          if ( isfield(trial_stats, 'trial_ids') )
            props.trial_ids = trial_stats.trial_ids;
            trial_stats = rmfield( trial_stats, 'trial_ids' );
          end
          props.trial_stats = trial_stats;
          prop_fields = fieldnames( props );
          for i = 1:numel(prop_fields)
            cont.(prop_fields{i}) = props.(prop_fields{i});
          end
        otherwise
          error( 'Unrecognized Container subclass ''%s''', kind );
      end
    end
    
  end
  
end