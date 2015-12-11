# 
# Log parser for YAZ Proxy
# 
# Copyright (c) 2015 University Of Helsinki (The National Library Of Finland)
# 
# This file is part of yaz-proxy-logparser
# 
# yaz-proxy-logparser is free software; you can redistribute it and/or modify it under the terms of either:
# a) the GNU General Public License as published by the Free Software Foundation; either version 3 (https://www.gnu.org/licenses/gpl-3.0.txt), or (at your option) any later version http://www.fsf.org/licenses/licenses.html#GNUGPL), or
# b) the "Artistic License" (http://dev.perl.org/licenses/artistic.html).
#
#

package YAZ::Proxy::LogParser;

use v5.10;
use strict;

use PerlIO::reverse;
use DateTime::Format::HTTP;

sub new
{

    my ($class, $config, $filters) = @_;
    my $self = {};

    bless $self, $class;

    #TODO: Validate config
    $self->{config} = $config;
    #TODO: Validate filters
    $self->{filters} = ref($filters) == 'ARRAY' ? $filters : @{$filters};

    if (defined($self->{config}->{dateStart})) {
	$self->{config}->{dateStart} = DateTime::Format::HTTP->parse_datetime($self->{config}->{dateStart}, 'local')->epoch();
    }
    if (defined($self->{config}->{dateEnd})) {
# Add one day to date so that comparison checks for the whole last day
	$self->{config}->{dateEnd} = DateTime::Format::HTTP->parse_datetime($self->{config}->{dateEnd}, 'local')->epoch() + 60 * 60 * 24;
    }

    return $self;

}

sub parse
{

    my $ip_current, my $file, my $skip;
    my @results = ();
    my ($self, $filename) = @_;

    if (defined($self->{config}->{dateEnd}) && defined($self->{config}->{skipFiles})) {
	
	if (!open($file, '<:reverse', $filename)) {
	    die($!);
	}

	_getEntries($file, sub {

	    if ($self->{config}->{dateStart} > shift) {	
		$skip = 1;
	    }

	    return 1;

	});

	close($file);

    }
	
    if (!$skip) {
       
	if (!open($file, $filename)) {
	    die($!);
	}
	
	_getEntries($file, sub {

	    my ($timestamp, $message) = @_;

	    if (defined($self->{config}->{dateStart}) && $timestamp < $self->{config}->{dateStart} || defined($self->{config}->{dateEnd}) && $timestamp > $self->{config}->{dateEnd}) {
		next;
	    } elsif ($message =~ /^New session tcp:(?:::ffff:)?(.*)$/) {
		
		#TODO: Check if IP is to be excluded
		$ip_current = $1;
		    
	    } elsif ($ip_current && $message =~ /^Search/) {
		
		my $skip;
		my $result = {
		    ip => $ip_current,
		    timestamp => $timestamp,
		    type => 'query',
		    query => $message =~ /^Search (.*)/ 
		};
		
		for my $filter (@{$self->{filters}}) {
		    if (&$filter($result)) {
			$skip = 1;
			last;
		    }
		}

		if (!$skip) {
		    push(@results, $result);
		}
	
        }
	    
	return undef;

     });	

    }

    return @results;

}

#Non-method subroutines
sub _getEntries
{

    my ($file, $callback) = @_;

    while (my $line = readline($file)) {
	if ($line =~ /New session/ || $line =~ /\d+\sSearch\s/) {
	    if ($line =~ /New session/ || $line =~ /\d+\sSearch\s/) {
		if ($line =~ /^\d+:\d+:\d+-\d+\/\d+ \[log\] ([\d]+):\d+ [\d ]*(.*)$/) {
		    if (&$callback($1, $2)) {
			last;
		    }
		}
	    }
	}
    }

}

1;

